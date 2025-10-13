// main.cpp
#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/DeleteObjectRequest.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
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
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;
using Ms = std::chrono::milliseconds;
using Us = std::chrono::microseconds;

static const char* kAllocTag = "s3-bench";

// ---------- utils: string, size parsing ----------
static inline std::string to_lower(std::string s) {
  for (char& c : s) c = std::tolower(static_cast<unsigned char>(c));
  return s;
}
static inline std::string to_upper(std::string s) {
  for (char& c : s) c = std::toupper(static_cast<unsigned char>(c));
  return s;
}
static uint64_t parse_size(const std::string& in) {
  std::string s = to_upper(in);
  // remove spaces
  s.erase(std::remove_if(s.begin(), s.end(), ::isspace), s.end());
  if (s.empty()) throw std::runtime_error("empty size");

  // find number and suffix
  size_t i = 0;
  while (i < s.size() && (std::isdigit(s[i]) || s[i] == '.')) i++;
  if (i == 0) throw std::runtime_error("invalid size: " + in);
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
  auto t = system_clock::to_time_t(system_clock::now());
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
  virtual uint64_t next() = 0;  // returns in [0, n-1]
};

struct SeqDistribution : IDistribution {
  uint64_t n;
  uint64_t cur;
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
    if (n == 0) return 0;
    return dist(rng);
  }
};

// Simple Zipf via precomputed CDF. Memory O(n).
struct ZipfDistribution : IDistribution {
  uint64_t n;
  double s;
  std::mt19937_64 rng;
  std::uniform_real_distribution<double> U{0.0, 1.0};
  std::vector<double> cdf;  // cdf[i] for rank i+1

  ZipfDistribution(uint64_t n_, double s_, uint64_t seed)
      : n(n_), s(s_), rng(seed) {
    if (n == 0) return;
    cdf.resize(n);
    long double Hn = 0.0L;
    for (uint64_t k = 1; k <= n; ++k) {
      Hn += 1.0L / std::pow((long double)k, (long double)s);
    }
    long double acc = 0.0L;
    for (uint64_t k = 1; k <= n; ++k) {
      acc += (1.0L / std::pow((long double)k, (long double)s)) / Hn;
      cdf[k - 1] = (double)acc;
    }
    // ensure last is exactly 1
    cdf.back() = 1.0;
  }
  uint64_t next() override {
    if (n == 0) return 0;
    double u = U(rng);
    auto it = std::lower_bound(cdf.begin(), cdf.end(), u);
    uint64_t rank_index =
        (uint64_t)std::distance(cdf.begin(), it);  // 0..n-1, rank=idx+1
    return rank_index;  // we treat addr index = rank_index
  }
};

// ----- zero/pattern stream (request body without holding huge memory) -----
class PatternBuf : public std::streambuf {
 public:
  explicit PatternBuf(uint64_t total_len, unsigned char byte = 0)
      : total(total_len), pattern(byte) {
    setp(nullptr, nullptr);        // no output
    setg(buffer, buffer, buffer);  // empty get area
  }

 protected:
  int_type underflow() override {
    if (pos() >= total) return traits_type::eof();

    // Fill buffer with min(kChunk, remaining)
    const uint64_t remaining = total - pos();
    const size_t n =
        static_cast<size_t>(std::min<uint64_t>(remaining, sizeof(buffer)));
    std::memset(buffer, pattern, n);

    // Expose freshly filled bytes
    setg(buffer, buffer, buffer + n);
    // We define "pos" as the position of gptr(); we'll advance it virtually on
    // seek.
    current_block_begin = logical;  // remember where this block starts
    logical += n;                   // logical end after this block
    return traits_type::to_int_type(*gptr());
  }

  pos_type seekoff(off_type off, std::ios_base::seekdir dir,
                   std::ios_base::openmode which) override {
    if (!(which & std::ios_base::in)) return pos_type(off_type(-1));
    long long base = 0;
    if (dir == std::ios_base::beg) {
      base = 0;
    } else if (dir == std::ios_base::cur) {
      base = static_cast<long long>(pos());
    } else if (dir == std::ios_base::end) {
      base = static_cast<long long>(total);
    }
    long long newpos = base + off;
    if (newpos < 0 || static_cast<uint64_t>(newpos) > total)
      return pos_type(off_type(-1));

    // Reset get area so the next underflow will refill from new position
    logical = static_cast<uint64_t>(newpos);
    current_block_begin = logical;
    setg(buffer, buffer, buffer);  // invalidate buffered data
    return pos_type(logical);
  }

  pos_type seekpos(pos_type sp, std::ios_base::openmode which) override {
    return seekoff(off_type(sp), std::ios_base::beg, which);
  }

 private:
  // Stream "logical" position is the position of the next byte to serve (i.e.,
  // gptr()).
  uint64_t pos() const {
    // If we currently have a buffer exposed, compute pos from it; otherwise
    // it's 'logical'.
    if (gptr() && egptr() && gptr() < egptr()) {
      return current_block_begin + static_cast<uint64_t>(gptr() - eback());
    }
    return logical;
  }

  static constexpr size_t kChunk = 64 * 1024;
  char buffer[kChunk];

  uint64_t total = 0;    // total length to present
  uint64_t logical = 0;  // next byte index to present
  uint64_t current_block_begin = 0;
  unsigned char pattern = 0;
};

class PatternStream : public Aws::IOStream {
 public:
  explicit PatternStream(uint64_t total_len, unsigned char byte = 0)
      : Aws::IOStream(nullptr), buf(total_len, byte) {
    this->rdbuf(&buf);  // attach buffer
    this->clear();      // clear badbit set by null-ctor so good() is true
  }

 private:
  PatternBuf buf;
};

// ---------- args / config ----------
struct Args {
  // required
  std::string op = "write";  // write | read-object | read-range
  std::string bucket;
  std::string region;

  // common
  std::string prefix = "bench/";
  uint64_t size = 1024;  // per-op size (bytes)
  int threads = 1;
  uint64_t total_ops = 1000;
  std::string pattern = "uniform";  // uniform|seq|zipf
  double zipf_s = 1.1;
  uint64_t addr_space = 1024;  // object keys or start-offset slots
  int connect_timeout_ms = 5000;
  int request_timeout_ms = 300000;
  int max_connections = 0;  // 0 -> auto = threads*2
  std::string endpoint;     // optional
  bool log_ops = false;
  std::string results_dir = "results";
  std::string sink_path = "/dev/null";  // for GET body

  // read-range
  std::string range_key = "range.bin";  // object name for range read
  uint64_t range_span =
      1024ull * 1024ull * 1024ull;  // total object length for range object

  // write/read-object
  std::string single_key;     // if non-empty, override key
  bool delete_after = false;  // delete objects written after test (caution!)
};

static void print_help() {
  std::cout
      << "Usage: s3_bench --op=write|read-object|read-range --bucket=... "
         "--region=... [options]\n"
         "Common options:\n"
         "  --prefix=bench/            Key prefix (default bench/)\n"
         "  --size=64KB                Per-op size (8B..128MB)\n"
         "  --threads=1                Concurrency threads\n"
         "  --total_ops=1000           Total operations across all threads\n"
         "  --pattern=uniform|seq|zipf Address/key distribution\n"
         "  --zipf_s=1.1               Zipf s parameter\n"
         "  --addr_space=1024          Address space (keys or start-offset "
         "slots)\n"
         "  --endpoint=...             Optional S3 endpoint override\n"
         "  --connect_timeout_ms=5000  Connect timeout\n"
         "  --request_timeout_ms=300000 Request timeout\n"
         "  --max_connections=0        0->auto=threads*2\n"
         "  --results_dir=results      Output CSV dir (summary.csv)\n"
         "  --log_ops=0|1              Per-op CSV log\n"
         "  --sink=/dev/null           Where to stream GET body (use NUL on "
         "Windows)\n"
         "\n"
         "Write/read-object options:\n"
         "  --single_key=name          If set, use prefix+name as the only "
         "key\n"
         "  --delete_after=0|1         Delete objects written in this run\n"
         "\n"
         "Read-range options:\n"
         "  --range_key=range.bin      Object name for range read (under "
         "prefix)\n"
         "  --range_span=2GB           Total object length for range object\n"
         "\n"
         "Examples:\n"
         "  s3_bench --op=write --bucket=my-bkt --region=ap-northeast-1 "
         "--size=64KB --threads=16 --total_ops=100000 --pattern=zipf "
         "--addr_space=4096\n"
         "  s3_bench --op=read-range --bucket=my-bkt --region=ap-northeast-1 "
         "--size=4KB --range_span=2GB --range_key=range.bin --threads=64 "
         "--total_ops=200000\n";
}

static bool parse_bool(const std::string& s) {
  std::string v = to_lower(s);
  return (v == "1" || v == "true" || v == "yes" || v == "on");
}

static Args parse_args(int argc, char** argv) {
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
      if (eq == std::string::npos) {
        kv[arg.substr(2)] = "1";
      } else {
        kv[arg.substr(2, eq - 2)] = arg.substr(eq + 1);
      }
    }
  }
  auto get = [&](const char* k, const char* def = nullptr) -> std::string {
    auto it = kv.find(k);
    if (it != kv.end()) return it->second;
    if (def) return def;
    return "";
  };

  if (!get("op").empty()) a.op = get("op");
  a.bucket = get("bucket");
  a.region = get("region");
  if (a.bucket.empty() || a.region.empty()) {
    std::cerr << "ERROR: --bucket and --region are required.\n";
    print_help();
    std::exit(2);
  }
  if (!get("prefix").empty()) a.prefix = get("prefix");
  if (!get("size").empty()) a.size = parse_size(get("size"));
  if (!get("threads").empty()) a.threads = std::stoi(get("threads"));
  if (!get("total_ops").empty()) a.total_ops = std::stoull(get("total_ops"));
  if (!get("pattern").empty()) a.pattern = get("pattern");
  if (!get("zipf_s").empty()) a.zipf_s = std::stod(get("zipf_s"));
  if (!get("addr_space").empty()) a.addr_space = std::stoull(get("addr_space"));
  if (!get("endpoint").empty()) a.endpoint = get("endpoint");
  if (!get("connect_timeout_ms").empty())
    a.connect_timeout_ms = std::stoi(get("connect_timeout_ms"));
  if (!get("request_timeout_ms").empty())
    a.request_timeout_ms = std::stoi(get("request_timeout_ms"));
  if (!get("max_connections").empty())
    a.max_connections = std::stoi(get("max_connections"));
  if (!get("results_dir").empty()) a.results_dir = get("results_dir");
  if (!get("log_ops").empty()) a.log_ops = parse_bool(get("log_ops"));
  if (!get("sink").empty()) a.sink_path = get("sink");

  if (!get("range_key").empty()) a.range_key = get("range_key");
  if (!get("range_span").empty()) a.range_span = parse_size(get("range_span"));

  if (!get("single_key").empty()) a.single_key = get("single_key");
  if (!get("delete_after").empty())
    a.delete_after = parse_bool(get("delete_after"));

  if (a.threads <= 0) a.threads = 1;
  if (a.max_connections <= 0) a.max_connections = std::max(2, a.threads * 2);
  return a;
}

// ---------- S3 client factory ----------
static std::shared_ptr<Aws::S3Crt::S3CrtClient> make_client(const Args& a) {
  Aws::Client::ClientConfiguration cfg;
  cfg.region = a.region.c_str();
  cfg.connectTimeoutMs = a.connect_timeout_ms;
  cfg.requestTimeoutMs = a.request_timeout_ms;
  cfg.maxConnections = a.max_connections;
  if (!a.endpoint.empty()) {
    cfg.endpointOverride = a.endpoint.c_str();
    cfg.scheme = Aws::Http::Scheme::HTTPS;
  }
  // Optional: cfg.enableTcpKeepAlive = true;

  auto client = std::make_shared<Aws::S3Crt::S3CrtClient>(cfg);
  return client;
}

// ---------- stats ----------
struct Stats {
  std::vector<uint64_t> lat_us;
  std::atomic<uint64_t> bytes{0};
  std::atomic<uint64_t> ok{0};
  std::atomic<uint64_t> err{0};
  std::mutex op_mutex;
  std::unique_ptr<std::ofstream> ops_csv;

  void open_ops_csv(const std::string& path) {
    ops_csv =
        std::make_unique<std::ofstream>(path, std::ios::out | std::ios::app);
    (*ops_csv) << "ts,op,ok,us,bytes,http_status,error\n";
  }
  void log_op(const std::string& op, bool ok_, uint64_t us, uint64_t bytes_,
              int http_status, const std::string& err_msg) {
    if (!ops_csv) return;
    std::lock_guard<std::mutex> lg(op_mutex);
    (*ops_csv) << now_iso() << "," << op << "," << (ok_ ? 1 : 0) << "," << us
               << "," << bytes_ << "," << http_status << "," << "\"" << err_msg
               << "\"" << "\n";
  }
};

static double percentile(std::vector<uint64_t>& v, double p) {
  if (v.empty()) return 0.0;
  std::sort(v.begin(), v.end());
  double pos = p * (v.size() - 1);
  size_t idx = (size_t)pos;
  double frac = pos - idx;
  double a = (double)v[idx];
  double b = (idx + 1 < v.size()) ? (double)v[idx + 1] : a;
  return a + (b - a) * frac;
}

// ---------- key naming ----------
static inline std::string key_for(const Args& a, uint64_t idx) {
  if (!a.single_key.empty()) return a.prefix + a.single_key;
  // include size in path to avoid naming collisions across sizes
  std::ostringstream ss;
  ss << a.prefix << "size-" << a.size << "/obj_" << idx;
  return ss.str();
}

// ---------- workers ----------
struct WorkerCtx {
  const Args* args;
  std::shared_ptr<Aws::S3Crt::S3CrtClient> client;
  std::unique_ptr<IDistribution> dist;
  uint64_t ops = 0;
  uint64_t thread_id = 0;
  uint64_t start_slot_mod = 0;  // for seq, allow different start
};

static std::unique_ptr<IDistribution> make_dist(const std::string& name,
                                                uint64_t n, double zipf_s,
                                                uint64_t seed,
                                                uint64_t seq_start = 0) {
  std::string t = to_lower(name);
  if (t == "seq" || t == "sequential") {
    return std::make_unique<SeqDistribution>(n, seq_start);
  } else if (t == "uniform") {
    return std::make_unique<UniformDistribution>(n, seed);
  } else if (t == "zipf" || t == "zipfian") {
    return std::make_unique<ZipfDistribution>(n, zipf_s, seed);
  } else {
    throw std::runtime_error("unknown pattern: " + name);
  }
}

static void run_write(WorkerCtx& w, Stats& st) {
  const auto& a = *w.args;
  for (uint64_t i = 0; i < w.ops; ++i) {
    uint64_t idx = w.dist->next();
    std::string key = key_for(a, idx);
    Aws::S3Crt::Model::PutObjectRequest req;
    req.SetBucket(a.bucket.c_str());
    req.SetKey(key.c_str());
    req.SetContentLength(static_cast<long long>(a.size));

    // simple payload stream (zero pattern)
    auto body = Aws::MakeShared<PatternStream>(kAllocTag, a.size, 0);
    if (!body || !body->good())
      throw std::runtime_error("pattern body not good()");
    req.SetBody(body);

    auto t0 = Clock::now();
    auto outcome = w.client->PutObject(req);
    auto t1 = Clock::now();
    uint64_t us = std::chrono::duration_cast<Us>(t1 - t0).count();

    if (outcome.IsSuccess()) {
      st.ok.fetch_add(1, std::memory_order_relaxed);
      st.bytes.fetch_add(a.size, std::memory_order_relaxed);
      st.lat_us.push_back(us);
      st.log_op("PUT", true, us, a.size, 200, "");
    } else {
      st.err.fetch_add(1, std::memory_order_relaxed);
      int http_status = (int)outcome.GetError().GetResponseCode();
      st.lat_us.push_back(us);
      st.log_op("PUT", false, us, 0, http_status,
                outcome.GetError().GetMessage().c_str());
    }
  }
}

static void run_read_object(WorkerCtx& w, Stats& st) {
  const auto& a = *w.args;
  for (uint64_t i = 0; i < w.ops; ++i) {
    uint64_t idx = w.dist->next();
    std::string key = key_for(a, idx);
    Aws::S3Crt::Model::GetObjectRequest req;
    req.SetBucket(a.bucket.c_str());
    req.SetKey(key.c_str());
    // Stream response to sink file (default /dev/null)
    req.SetResponseStreamFactory([&]() {
      return Aws::New<Aws::FStream>(
          kAllocTag, a.sink_path.c_str(),
          std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
    });

    auto t0 = Clock::now();
    auto outcome = w.client->GetObject(req);
    auto t1 = Clock::now();
    uint64_t us = std::chrono::duration_cast<Us>(t1 - t0).count();

    if (outcome.IsSuccess()) {
      // If object size differs from a.size, we still count the actual bytes
      // (ContentLength)
      auto& result = outcome.GetResult();
      uint64_t bytes_got = (uint64_t)result.GetContentLength();
      st.ok.fetch_add(1, std::memory_order_relaxed);
      st.bytes.fetch_add(bytes_got, std::memory_order_relaxed);
      st.lat_us.push_back(us);
      st.log_op("GET", true, us, bytes_got, 200, "");
    } else {
      st.err.fetch_add(1, std::memory_order_relaxed);
      int http_status = (int)outcome.GetError().GetResponseCode();
      st.lat_us.push_back(us);
      st.log_op("GET", false, us, 0, http_status,
                outcome.GetError().GetMessage().c_str());
    }
  }
}

static void run_read_range(WorkerCtx& w, Stats& st) {
  const auto& a = *w.args;
  // For range, slots represent valid start offsets in [0, range_span - size]
  if (a.range_span < a.size) {
    throw std::runtime_error("range_span must be >= size");
  }
  uint64_t domain = a.range_span - a.size + 1;
  // If addr_space is set smaller than domain, we map slot -> offset via stride
  uint64_t n = std::min<uint64_t>(a.addr_space, domain);
  uint64_t stride = domain / n;

  for (uint64_t i = 0; i < w.ops; ++i) {
    uint64_t slot = w.dist->next();  // 0..n-1
    uint64_t offset = slot * stride;
    uint64_t end = offset + a.size - 1;

    Aws::S3Crt::Model::GetObjectRequest req;
    req.SetBucket(a.bucket.c_str());
    std::string key = a.prefix + a.range_key;
    req.SetKey(key.c_str());

    std::ostringstream rg;
    rg << "bytes=" << offset << "-" << end;
    req.SetRange(rg.str().c_str());

    req.SetResponseStreamFactory([&]() {
      return Aws::New<Aws::FStream>(
          kAllocTag, a.sink_path.c_str(),
          std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
    });

    auto t0 = Clock::now();
    auto outcome = w.client->GetObject(req);
    auto t1 = Clock::now();
    uint64_t us = std::chrono::duration_cast<Us>(t1 - t0).count();

    if (outcome.IsSuccess()) {
      st.ok.fetch_add(1, std::memory_order_relaxed);
      st.bytes.fetch_add(a.size, std::memory_order_relaxed);
      st.lat_us.push_back(us);
      st.log_op("GETR", true, us, a.size, 206, "");
    } else {
      st.err.fetch_add(1, std::memory_order_relaxed);
      int http_status = (int)outcome.GetError().GetResponseCode();
      st.lat_us.push_back(us);
      st.log_op("GETR", false, us, 0, http_status,
                outcome.GetError().GetMessage().c_str());
    }
  }
}

// ---------- delete helper (optional) ----------
static void delete_written_keys(
    const Args& a, const std::shared_ptr<Aws::S3Crt::S3CrtClient>& client) {
  if (!a.single_key.empty()) {
    Aws::S3Crt::Model::DeleteObjectRequest req;
    req.SetBucket(a.bucket.c_str());
    req.SetKey((a.prefix + a.single_key).c_str());
    client->DeleteObject(req);
    return;
  }
  for (uint64_t i = 0; i < a.addr_space; ++i) {
    Aws::S3Crt::Model::DeleteObjectRequest req;
    req.SetBucket(a.bucket.c_str());
    req.SetKey(key_for(a, i).c_str());
    client->DeleteObject(req);
  }
}

// ---------- results ----------
static void ensure_dir(const std::string& dir) {
  std::string cmd = "mkdir -p \"" + dir + "\"";
  std::ignore = std::system(cmd.c_str());
}

static void append_summary(const Args& a, const Stats& st, double seconds) {
  ensure_dir(a.results_dir);
  std::string path = a.results_dir + "/summary.csv";
  bool newfile = false;
  {
    std::ifstream in(path);
    if (!in.good()) newfile = true;
  }
  std::ofstream out(path, std::ios::out | std::ios::app);
  if (newfile) {
    out << "ts,op,region,bucket,prefix,size,threads,total_ops,ok,err,seconds,"
           "ops_per_s,MB_per_s,lat_p50_ms,lat_p90_ms,lat_p99_ms,pattern,zipf_s,"
           "addr_space,range_span,range_key\n";
  }
  // compute quantiles
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
      << "," << a.addr_space << "," << a.range_span << ","
      << (a.op == "read-range" ? (a.prefix + a.range_key) : "") << "\n";
}

int main(int argc, char** argv) {
  try {
    Args a = parse_args(argc, argv);

    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
      auto client = make_client(a);

      Stats stats;
      stats.lat_us.reserve(a.total_ops);
      if (a.log_ops) {
        ensure_dir(a.results_dir);
        std::string ops_path = a.results_dir + "/ops.csv";
        stats.open_ops_csv(ops_path);
      }

      // workers
      std::vector<std::thread> ths;
      std::vector<std::unique_ptr<WorkerCtx>> ctxs;
      ctxs.reserve(a.threads);
      uint64_t base_ops = a.total_ops / a.threads;
      uint64_t rem = a.total_ops % a.threads;

      auto t0 = Clock::now();
      for (int t = 0; t < a.threads; ++t) {
        auto ctx = std::make_unique<WorkerCtx>();
        ctx->args = &a;
        ctx->client = client;
        ctx->ops = base_ops + (t < (int)rem ? 1 : 0);
        ctx->thread_id = (uint64_t)t;
        uint64_t seed = 0xdeadbeefULL ^ ((uint64_t)t << 32) ^ (uint64_t)std::random_device{}();
        uint64_t seq_start =
            (a.addr_space > 0) ? ((a.addr_space / a.threads) * t) % a.addr_space : 0;
        uint64_t n =
            (a.op == "read-range")
                ? std::min<uint64_t>(
                      a.addr_space,
                      (a.range_span >= a.size ? a.range_span - a.size + 1 : 1))
                : a.addr_space;
        if (n == 0) n = 1;
        ctx->dist = make_dist(a.pattern, n, a.zipf_s, seed, seq_start);

        ctxs.push_back(std::move(ctx));
      }

      auto t0 = Clock::now();
      // 再启动线程，捕获稳定指针
      for (int t = 0; t < a.threads; ++t) {
        WorkerCtx* ctxp = ctxs[t].get();
        ths.emplace_back([&, ctxp]() {
          if (a.op == "write") {
            run_write(*ctxp, stats);
          } else if (a.op == "read-object") {
            run_read_object(*ctxp, stats);
          } else if (a.op == "read-range") {
            run_read_range(*ctxp, stats);
          } else {
            throw std::runtime_error("unknown op: " + a.op);
          }
        });
      }

      for (auto& th : ths) th.join();
      auto t1 = Clock::now();
      double seconds = std::chrono::duration<double>(t1 - t0).count();

      append_summary(a, stats, seconds);

      if (a.op == "write" && a.delete_after) {
        std::cerr << "Deleting written keys...\n";
        delete_written_keys(a, client);
      }
    }
    Aws::ShutdownAPI(options);
  } catch (const std::exception& ex) {
    std::cerr << "FATAL: " << ex.what() << "\n";
    return 1;
  }
  return 0;
}