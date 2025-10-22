// async.cpp
#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/DeleteObjectRequest.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

using Clock = std::chrono::steady_clock;
using Ms = std::chrono::milliseconds;
using Us = std::chrono::microseconds;

static const char *kAllocTag = "s3-bench-async";

// ---------- utils ----------
static inline std::string to_lower(std::string s) {
  for (char &c : s)
    c = std::tolower(static_cast<unsigned char>(c));
  return s;
}
static inline std::string to_upper(std::string s) {
  for (char &c : s)
    c = std::toupper(static_cast<unsigned char>(c));
  return s;
}
static uint64_t parse_size(const std::string &in) {
  std::string s = to_upper(in);
  s.erase(std::remove_if(s.begin(), s.end(), ::isspace), s.end());
  if (s.empty())
    throw std::runtime_error("empty size");
  size_t i = 0;
  while (i < s.size() && (std::isdigit(s[i]) || s[i] == '.'))
    i++;
  if (i == 0)
    throw std::runtime_error("invalid size: " + in);
  double val = std::stod(s.substr(0, i));
  std::string suf = (i < s.size() ? s.substr(i) : "");
  uint64_t mul = 1;
  if (suf == "" || suf == "B")
    mul = 1;
  else if (suf == "K" || suf == "KB" || suf == "KIB")
    mul = 1024ull;
  else if (suf == "M" || suf == "MB" || suf == "MIB")
    mul = 1024ull * 1024ull;
  else if (suf == "G" || suf == "GB" || suf == "GIB")
    mul = 1024ull * 1024ull * 1024ull;
  else if (suf == "T" || suf == "TB" || suf == "TIB")
    mul = 1024ull * 1024ull * 1024ull * 1024ull;
  else
    throw std::runtime_error("unknown size suffix: " + suf);
  long double bytes = val * static_cast<long double>(mul);
  if (bytes > std::numeric_limits<uint64_t>::max())
    throw std::runtime_error("size too large");
  return static_cast<uint64_t>(bytes + 0.5L);
}
static inline std::string now_iso() {
  using namespace std::chrono;
  auto t =
      std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  std::tm tm{};
#ifdef _WIN32
  gmtime_s(&tm, &t);
#else
  gmtime_r(&t, &tm);
#endif
  char buf[64];
  std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
  return buf;
}

// ---------- distributions ----------
struct IDistribution {
  virtual ~IDistribution() = default;
  virtual uint64_t next() = 0;
};
struct SeqDistribution : IDistribution {
  uint64_t n, cur;
  SeqDistribution(uint64_t n_, uint64_t start = 0)
      : n(n_), cur(start % (n_ ? n_ : 1)) {}
  uint64_t next() override {
    uint64_t v = cur;
    cur = (cur + 1) % (n ? n : 1);
    return v;
  }
};
struct UniformDistribution : IDistribution {
  uint64_t n;
  std::mt19937_64 rng;
  std::uniform_int_distribution<uint64_t> dist;
  UniformDistribution(uint64_t n_, uint64_t seed)
      : n(n_), rng(seed), dist(0, n_ ? n_ - 1 : 0) {}
  uint64_t next() override {
    if (n == 0)
      return 0;
    return dist(rng);
  }
};
struct ZipfDistribution : IDistribution {
  uint64_t n;
  double s;
  std::mt19937_64 rng;
  std::uniform_real_distribution<double> U{0.0, 1.0};
  std::vector<double> cdf;
  ZipfDistribution(uint64_t n_, double s_, uint64_t seed)
      : n(n_), s(s_), rng(seed) {
    if (n == 0)
      return;
    cdf.resize(n);
    long double Hn = 0.0L;
    for (uint64_t k = 1; k <= n; ++k)
      Hn += 1.0L / std::pow((long double)k, (long double)s);
    long double acc = 0.0L;
    for (uint64_t k = 1; k <= n; ++k) {
      acc += (1.0L / std::pow((long double)k, (long double)s)) / Hn;
      cdf[k - 1] = (double)acc;
    }
    cdf.back() = 1.0;
  }
  uint64_t next() override {
    if (n == 0)
      return 0;
    double u = U(rng);
    auto it = std::lower_bound(cdf.begin(), cdf.end(), u);
    return (uint64_t)std::distance(cdf.begin(), it);
  }
};

class PatternBuf : public std::streambuf {
public:
  explicit PatternBuf(uint64_t total_len, unsigned char byte = 0)
      : total(total_len), pattern(byte) {
    setp(nullptr, nullptr);
    setg(buffer, buffer, buffer);
  }

protected:
  int_type underflow() override {
    if (pos() >= total)
      return traits_type::eof();
    const uint64_t remaining = total - pos();
    const size_t n =
        static_cast<size_t>(std::min<uint64_t>(remaining, sizeof(buffer)));
    std::memset(buffer, pattern, n);
    setg(buffer, buffer, buffer + n);
    current_block_begin = logical;
    logical += n;
    return traits_type::to_int_type(*gptr());
  }
  pos_type seekoff(off_type off, std::ios_base::seekdir dir,
                   std::ios_base::openmode which) override {
    if (!(which & std::ios_base::in))
      return pos_type(off_type(-1));
    long long base = 0;
    if (dir == std::ios_base::beg)
      base = 0;
    else if (dir == std::ios_base::cur)
      base = static_cast<long long>(pos());
    else if (dir == std::ios_base::end)
      base = static_cast<long long>(total);
    long long newpos = base + off;
    if (newpos < 0 || static_cast<uint64_t>(newpos) > total)
      return pos_type(off_type(-1));
    logical = static_cast<uint64_t>(newpos);
    current_block_begin = logical;
    setg(buffer, buffer, buffer);
    return pos_type(logical);
  }
  pos_type seekpos(pos_type sp, std::ios_base::openmode which) override {
    return seekoff(off_type(sp), std::ios_base::beg, which);
  }

private:
  uint64_t pos() const {
    if (gptr() && egptr() && gptr() < egptr())
      return current_block_begin + static_cast<uint64_t>(gptr() - eback());
    return logical;
  }
  static constexpr size_t kChunk = 64 * 1024;
  char buffer[kChunk];
  uint64_t total = 0, logical = 0, current_block_begin = 0;
  unsigned char pattern = 0;
};
class PatternStream : public Aws::IOStream {
public:
  explicit PatternStream(uint64_t total_len, unsigned char byte = 0)
      : Aws::IOStream(nullptr), buf(total_len, byte) {
    this->rdbuf(&buf);
    this->clear();
  }

private:
  PatternBuf buf;
};

// ---------- args / config ----------
struct Args {
  std::string op = "write-async"; // write-async | read-object-async
  std::string bucket;
  std::string region;

  std::string prefix = "bench/";
  uint64_t size = 1024;
  int threads = 64;
  uint64_t total_ops = 10000;
  std::string pattern = "uniform";
  double zipf_s = 1.1;
  uint64_t addr_space = 1024;
  int connect_timeout_ms = 5000;
  int request_timeout_ms = 300000;
  int max_connections = 0; // 0 -> auto = threads*2
  std::string endpoint;
  bool log_ops = false;
  std::string results_dir = "results";
  std::string sink_path = "/dev/null";

  std::string single_key;
  bool delete_after = false;
};

static void print_help() {
  std::cout
      << "Usage: s3_bench_async --op=write-async|read-object-async "
         "--bucket=... --region=... [options]\n"
         "Common options:\n"
         "  --prefix=bench/            Key prefix\n"
         "  --size=64KB                Per-op size\n"
         "  --threads=64               Async in-flight concurrency window\n"
         "  --total_ops=100000         Total operations\n"
         "  --pattern=uniform|seq|zipf Distribution for key selection\n"
         "  --zipf_s=1.1               Zipf s\n"
         "  --addr_space=1024          Key space size\n"
         "  --endpoint=...             Optional S3 endpoint override\n"
         "  --connect_timeout_ms=5000  Connect timeout\n"
         "  --request_timeout_ms=300000 Request timeout\n"
         "  --max_connections=0        0->auto=threads*2\n"
         "  --results_dir=results      Output CSV dir\n"
         "  --log_ops=0|1              Per-op CSV log\n"
         "  --sink=/dev/null           GET response sink (use NUL on Windows)\n"
         "  --single_key=name          If set, use prefix+name as the only "
         "key\n"
         "  --delete_after=0|1         Delete written objects after test\n";
}

static bool parse_bool(const std::string &s) {
  std::string v = to_lower(s);
  return (v == "1" || v == "true" || v == "yes" || v == "on");
}
static Args parse_args(int argc, char **argv) {
  Args a;
  std::map<std::string, std::string> kv;
  for (int i = 1; i < argc; i++) {
    std::string arg(argv[i]);
    if (arg == "-h" || arg == "--help") {
      print_help();
      std::exit(0);
    }
    if (arg.rfind("--", 0) == 0) {
      size_t eq = arg.find('=');
      if (eq == std::string::npos)
        kv[arg.substr(2)] = "1";
      else
        kv[arg.substr(2, eq - 2)] = arg.substr(eq + 1);
    }
  }
  auto get = [&](const char *k, const char *def = nullptr) -> std::string {
    auto it = kv.find(k);
    if (it != kv.end())
      return it->second;
    if (def)
      return def;
    return "";
  };
  if (!get("op").empty())
    a.op = get("op");
  a.bucket = get("bucket");
  a.region = get("region");
  if (a.bucket.empty() || a.region.empty()) {
    std::cerr << "ERROR: --bucket and --region are required.\n";
    print_help();
    std::exit(2);
  }
  if (!get("prefix").empty())
    a.prefix = get("prefix");
  if (!get("size").empty())
    a.size = parse_size(get("size"));
  if (!get("threads").empty())
    a.threads = std::stoi(get("threads"));
  if (!get("total_ops").empty())
    a.total_ops = std::stoull(get("total_ops"));
  if (!get("pattern").empty())
    a.pattern = get("pattern");
  if (!get("zipf_s").empty())
    a.zipf_s = std::stod(get("zipf_s"));
  if (!get("addr_space").empty())
    a.addr_space = std::stoull(get("addr_space"));
  if (!get("endpoint").empty())
    a.endpoint = get("endpoint");
  if (!get("connect_timeout_ms").empty())
    a.connect_timeout_ms = std::stoi(get("connect_timeout_ms"));
  if (!get("request_timeout_ms").empty())
    a.request_timeout_ms = std::stoi(get("request_timeout_ms"));
  if (!get("max_connections").empty())
    a.max_connections = std::stoi(get("max_connections"));
  if (!get("results_dir").empty())
    a.results_dir = get("results_dir");
  if (!get("log_ops").empty())
    a.log_ops = parse_bool(get("log_ops"));
  if (!get("sink").empty())
    a.sink_path = get("sink");
  if (!get("single_key").empty())
    a.single_key = get("single_key");
  if (!get("delete_after").empty())
    a.delete_after = parse_bool(get("delete_after"));

  if (a.threads <= 0)
    a.threads = 1;
  if (a.max_connections <= 0)
    a.max_connections = std::max(2, a.threads * 2);
  return a;
}

// ---------- client ----------
static std::shared_ptr<Aws::S3Crt::S3CrtClient> make_client(const Args &a) {
  Aws::Client::ClientConfiguration cfg;
  cfg.region = a.region.c_str();
  cfg.connectTimeoutMs = a.connect_timeout_ms;
  cfg.requestTimeoutMs = a.request_timeout_ms;
  cfg.maxConnections = a.max_connections;
  if (!a.endpoint.empty()) {
    cfg.endpointOverride = a.endpoint.c_str();
    cfg.scheme = Aws::Http::Scheme::HTTPS;
  }
  // cfg.enableTcpKeepAlive = true;
  auto client = std::make_shared<Aws::S3Crt::S3CrtClient>(cfg);
  return client;
}

// ---------- stats ----------
struct Stats {
  std::vector<uint64_t> lat_us;
  std::mutex lat_mu;

  std::atomic<uint64_t> bytes{0};
  std::atomic<uint64_t> ok{0};
  std::atomic<uint64_t> err{0};

  std::mutex op_mutex;
  std::unique_ptr<std::ofstream> ops_csv;
  void open_ops_csv(const std::string &path) {
    ops_csv =
        std::make_unique<std::ofstream>(path, std::ios::out | std::ios::app);
    (*ops_csv) << "ts,op,ok,us,bytes,http_status,error\n";
  }
  void push_latency(uint64_t us) {
    std::lock_guard<std::mutex> g(lat_mu);
    lat_us.push_back(us);
  }
  void log_op(const std::string &op, bool ok_, uint64_t us, uint64_t bytes_,
              int http_status, const std::string &err_msg) {
    if (!ops_csv)
      return;
    std::lock_guard<std::mutex> lg(op_mutex);
    (*ops_csv) << now_iso() << "," << op << "," << (ok_ ? 1 : 0) << "," << us
               << "," << bytes_ << "," << http_status << "," << "\"" << err_msg
               << "\"" << "\n";
  }
};
static double percentile(std::vector<uint64_t> &v, double p) {
  if (v.empty())
    return 0.0;
  std::sort(v.begin(), v.end());
  double pos = p * (v.size() - 1);
  size_t idx = (size_t)pos;
  double frac = pos - idx;
  double a = (double)v[idx];
  double b = (idx + 1 < v.size()) ? (double)v[idx + 1] : a;
  return a + (b - a) * frac;
}

// ---------- key naming ----------
static inline std::string key_for(const Args &a, uint64_t idx) {
  if (!a.single_key.empty())
    return a.prefix + a.single_key;
  std::ostringstream ss;
  ss << a.prefix << "size-" << a.size << "/obj_" << idx;
  return ss.str();
}

// ---------- results ----------
static void ensure_dir(const std::string &dir) {
#ifdef _WIN32
  std::string cmd = "powershell -NoProfile -Command \"New-Item -ItemType "
                    "Directory -Force -Path '" +
                    dir + "' | Out-Null\"";
#else
  std::string cmd = "mkdir -p \"" + dir + "\"";
#endif
  std::ignore = std::system(cmd.c_str());
}
static void append_summary(const Args &a, const Stats &st, double seconds) {
  ensure_dir(a.results_dir);
  std::string path = a.results_dir + "/summary.csv";
  bool newfile = false;
  {
    std::ifstream in(path);
    if (!in.good())
      newfile = true;
  }
  std::ofstream out(path, std::ios::out | std::ios::app);
  if (newfile) {
    out << "ts,op,region,bucket,prefix,size,threads,total_ops,ok,err,seconds,"
           "ops_per_s,MB_per_s,lat_p50_ms,lat_p90_ms,lat_p99_ms,pattern,zipf_s,"
           "addr_space,range_span,range_key\n";
  }
  auto v = st.lat_us;
  double p50 = percentile(v, 0.50) / 1000.0;
  double p90 = percentile(v, 0.90) / 1000.0;
  double p99 = percentile(v, 0.99) / 1000.0;
  double ops_s = (seconds > 0) ? (double)st.ok / seconds : 0.0;
  double mb_s =
      (seconds > 0) ? ((double)st.bytes / (1024.0 * 1024.0)) / seconds : 0.0;

  out << now_iso() << "," << a.op << "," << a.region << "," << a.bucket << ","
      << a.prefix << "," << a.size << "," << a.threads << "," << a.total_ops
      << "," << st.ok.load() << "," << st.err.load() << "," << std::fixed
      << std::setprecision(3) << seconds << "," << std::fixed
      << std::setprecision(3) << ops_s << "," << std::fixed
      << std::setprecision(3) << mb_s << "," << std::fixed
      << std::setprecision(3) << p50 << "," << std::fixed
      << std::setprecision(3) << p90 << "," << std::fixed
      << std::setprecision(3) << p99 << "," << a.pattern << "," << a.zipf_s
      << "," << a.addr_space << "," << 0 << "," << "" << "\n";
}

// ---------- dist factory ----------
static std::unique_ptr<IDistribution> make_dist(const std::string &name,
                                                uint64_t n, double zipf_s,
                                                uint64_t seed,
                                                uint64_t seq_start = 0) {
  std::string t = to_lower(name);
  if (t == "seq" || t == "sequential")
    return std::make_unique<SeqDistribution>(n, seq_start);
  else if (t == "uniform")
    return std::make_unique<UniformDistribution>(n, seed);
  else if (t == "zipf" || t == "zipfian")
    return std::make_unique<ZipfDistribution>(n, zipf_s, seed);
  else
    throw std::runtime_error("unknown pattern: " + name);
}

// ---------- async coordinator ----------
struct AsyncCounters {
  std::atomic<uint64_t> launched{0};
  std::atomic<uint64_t> completed{0};
  std::atomic<uint64_t> inflight{0};
  uint64_t total = 0;
  uint64_t window = 0;
  std::mutex mu;
  std::condition_variable cv;
};

struct OpContext : public Aws::Client::AsyncCallerContext {
  Clock::time_point t0;
  uint64_t expect_bytes = 0;
  std::shared_ptr<Aws::IOStream> body_to_keep_alive; // for PUT body lifetime
  AsyncCounters *ctrs = nullptr;
  Stats *stats = nullptr;
  std::string op_name; // "PUT" / "GET"
};

// ---------- async ops ----------
static void
run_write_async(const Args &a,
                const std::shared_ptr<Aws::S3Crt::S3CrtClient> &client,
                Stats &stats) {
  AsyncCounters ctrs;
  ctrs.total = a.total_ops;
  ctrs.window = static_cast<uint64_t>(a.threads);

  uint64_t n = a.addr_space;
  if (n == 0)
    n = 1;
  uint64_t seed = 0xabcddcbaULL ^ (uint64_t)std::random_device{}();
  auto dist = make_dist(a.pattern, n, a.zipf_s, seed, 0);

  auto submit_one = [&](uint64_t idx_slot) {
    const std::string key = key_for(a, idx_slot);
    Aws::S3Crt::Model::PutObjectRequest req;
    req.SetBucket(a.bucket.c_str());
    req.SetKey(key.c_str());
    req.SetContentLength(static_cast<long long>(a.size));
    auto body = Aws::MakeShared<PatternStream>(kAllocTag, a.size, 0);
    if (!body || !body->good())
      throw std::runtime_error("pattern body not good()");
    req.SetBody(body);

    auto ctx = Aws::MakeShared<OpContext>(kAllocTag);
    ctx->t0 = Clock::now();
    ctx->expect_bytes = a.size;
    ctx->body_to_keep_alive = body;
    ctx->ctrs = &ctrs;
    ctx->stats = &stats;
    ctx->op_name = "PUT";

    client->PutObjectAsync(
        req,
        [&, body](const Aws::S3Crt::S3CrtClient *,
                  const Aws::S3Crt::Model::PutObjectRequest &,
                  const Aws::S3Crt::Model::PutObjectOutcome &outcome,
                  const std::shared_ptr<const Aws::Client::AsyncCallerContext>
                      &base_ctx) {
          auto ctxp = std::static_pointer_cast<const OpContext>(base_ctx);
          uint64_t us =
              std::chrono::duration_cast<Us>(Clock::now() - ctxp->t0).count();

          if (outcome.IsSuccess()) {
            ctxp->stats->ok.fetch_add(1, std::memory_order_relaxed);
            ctxp->stats->bytes.fetch_add(ctxp->expect_bytes,
                                         std::memory_order_relaxed);
            ctxp->stats->push_latency(us);
            ctxp->stats->log_op("PUT", true, us, ctxp->expect_bytes, 200, "");
          } else {
            ctxp->stats->err.fetch_add(1, std::memory_order_relaxed);
            int http_status = (int)outcome.GetError().GetResponseCode();
            ctxp->stats->push_latency(us);
            ctxp->stats->log_op("PUT", false, us, 0, http_status,
                                outcome.GetError().GetMessage().c_str());
          }

          ctxp->ctrs->completed.fetch_add(1, std::memory_order_relaxed);
          ctxp->ctrs->inflight.fetch_sub(1, std::memory_order_relaxed);
          {
            std::lock_guard<std::mutex> lg(ctxp->ctrs->mu);
            ctxp->ctrs->cv.notify_one();
          }
        },
        ctx);
  };

  auto t0 = Clock::now();
  while (ctrs.completed.load(std::memory_order_relaxed) < ctrs.total) {
    while (ctrs.launched.load(std::memory_order_relaxed) < ctrs.total &&
           ctrs.inflight.load(std::memory_order_relaxed) < ctrs.window) {
      uint64_t idx = dist->next();
      ctrs.launched.fetch_add(1, std::memory_order_relaxed);
      ctrs.inflight.fetch_add(1, std::memory_order_relaxed);
      submit_one(idx);
    }
    std::unique_lock<std::mutex> lk(ctrs.mu);
    ctrs.cv.wait_for(lk, std::chrono::milliseconds(1));
  }
  auto t1 = Clock::now();
  double seconds = std::chrono::duration<double>(t1 - t0).count();
  append_summary(a, stats, seconds);

  if (a.delete_after && a.single_key.empty()) {
    std::cerr << "Deleting written keys...\n";
    for (uint64_t i = 0; i < a.addr_space; ++i) {
      Aws::S3Crt::Model::DeleteObjectRequest dr;
      dr.SetBucket(a.bucket.c_str());
      dr.SetKey(key_for(a, i).c_str());
      client->DeleteObject(dr);
    }
  } else if (a.delete_after && !a.single_key.empty()) {
    Aws::S3Crt::Model::DeleteObjectRequest dr;
    dr.SetBucket(a.bucket.c_str());
    dr.SetKey((a.prefix + a.single_key).c_str());
    client->DeleteObject(dr);
  }
}

static void
run_read_object_async(const Args &a,
                      const std::shared_ptr<Aws::S3Crt::S3CrtClient> &client,
                      Stats &stats) {
  AsyncCounters ctrs;
  ctrs.total = a.total_ops;
  ctrs.window = static_cast<uint64_t>(a.threads);

  uint64_t n = a.addr_space;
  if (n == 0)
    n = 1;
  uint64_t seed = 0x1122334455ULL ^ (uint64_t)std::random_device{}();
  auto dist = make_dist(a.pattern, n, a.zipf_s, seed, 0);

  auto submit_one = [&](uint64_t idx_slot) {
    const std::string key = key_for(a, idx_slot);

    Aws::S3Crt::Model::GetObjectRequest req;
    req.SetBucket(a.bucket.c_str());
    req.SetKey(key.c_str());
    req.SetResponseStreamFactory([&]() {
      return Aws::New<Aws::FStream>(kAllocTag, a.sink_path.c_str(),
                                    std::ios_base::out | std::ios_base::binary |
                                        std::ios_base::trunc);
    });

    auto ctx = Aws::MakeShared<OpContext>(kAllocTag);
    ctx->t0 = Clock::now();
    ctx->expect_bytes = 0; // we'll use ContentLength from outcome
    ctx->ctrs = &ctrs;
    ctx->stats = &stats;
    ctx->op_name = "GET";

    client->GetObjectAsync(
        req,
        [&](const Aws::S3Crt::S3CrtClient *,
            const Aws::S3Crt::Model::GetObjectRequest &,
            const Aws::S3Crt::Model::GetObjectOutcome &outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext>
                &base_ctx) {
          auto ctxp = std::static_pointer_cast<const OpContext>(base_ctx);
          uint64_t us =
              std::chrono::duration_cast<Us>(Clock::now() - ctxp->t0).count();

          if (outcome.IsSuccess()) {
            const auto &res = outcome.GetResult();
            uint64_t got = (uint64_t)res.GetContentLength();
            ctxp->stats->ok.fetch_add(1, std::memory_order_relaxed);
            ctxp->stats->bytes.fetch_add(got, std::memory_order_relaxed);
            ctxp->stats->push_latency(us);
            ctxp->stats->log_op("GET", true, us, got, 200, "");
          } else {
            ctxp->stats->err.fetch_add(1, std::memory_order_relaxed);
            int http_status = (int)outcome.GetError().GetResponseCode();
            ctxp->stats->push_latency(us);
            ctxp->stats->log_op("GET", false, us, 0, http_status,
                                outcome.GetError().GetMessage().c_str());
          }

          ctxp->ctrs->completed.fetch_add(1, std::memory_order_relaxed);
          ctxp->ctrs->inflight.fetch_sub(1, std::memory_order_relaxed);
          {
            std::lock_guard<std::mutex> lg(ctxp->ctrs->mu);
            ctxp->ctrs->cv.notify_one();
          }
        },
        ctx);
  };

  auto t0 = Clock::now();
  while (ctrs.completed.load(std::memory_order_relaxed) < ctrs.total) {
    while (ctrs.launched.load(std::memory_order_relaxed) < ctrs.total &&
           ctrs.inflight.load(std::memory_order_relaxed) < ctrs.window) {
      uint64_t idx = dist->next();
      ctrs.launched.fetch_add(1, std::memory_order_relaxed);
      ctrs.inflight.fetch_add(1, std::memory_order_relaxed);
      submit_one(idx);
    }
    std::unique_lock<std::mutex> lk(ctrs.mu);
    ctrs.cv.wait_for(lk, std::chrono::milliseconds(1));
  }
  auto t1 = Clock::now();
  double seconds = std::chrono::duration<double>(t1 - t0).count();
  append_summary(a, stats, seconds);
}

// ---------- main ----------
int main(int argc, char **argv) {
  try {
    Args a = parse_args(argc, argv);
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
      auto client = make_client(a);
      Stats stats;
      if (a.log_ops) {
        ensure_dir(a.results_dir);
        stats.open_ops_csv(a.results_dir + "/ops.csv");
      }

      if (a.op == "write-async") {
        run_write_async(a, client, stats);
      } else if (a.op == "read-object-async") {
        run_read_object_async(a, client, stats);
      } else {
        std::cerr << "Unknown op: " << a.op << "\n";
        Aws::ShutdownAPI(options);
        return 2;
      }

      std::cerr << "Done. OK=" << stats.ok << " ERR=" << stats.err << "\n";
    }
    Aws::ShutdownAPI(options);
  } catch (const std::exception &ex) {
    std::cerr << "FATAL: " << ex.what() << "\n";
    return 1;
  }
  return 0;
}