#!/usr/bin/env bash
set -euo pipefail

# 基本参数
BUCKET="bench-fjy1"
REGION="ap-northeast-2"
PREFIX="bench/"
RESULTS_DIR="results"
SINK="/dev/null"
ZIPF_S="0.99"

# SIZES=(1KB 16KB 128KB 1MB 16MB 64MB 128MB)
SIZES=(8B 16B 32B 64B 128B 256B 512B 1KB 4KB 8KB 16KB 32KB 64KB 128KB 256KB 512KB 1MB 2MB 4MB 8MB 16MB 32MB 64MB 128MB)

PATTERNS=(seq uniform zipf)

# Derive address space per size (cap the number of distinct keys per size)
addr_space_for_size() {
  local s="$1"
  case "$s" in
    8B|16B|32B|64B|128B|256B|512B|1KB|4KB|8KB|16KB|32KB|64KB)
      echo 2048         # fewer tiny-object keys; saves on listing/cleanup and metadata churn
      ;;
    128KB|256KB|512KB|1MB|2MB|4MB|8MB)
      echo 1024
      ;;
    16MB|32MB|64MB)
      echo 256
      ;;
    128MB)
      echo 64
      ;;
    *)
      echo 1024
      ;;
  esac
}

# Size-aware ops to keep runtime sane on 8 vCPUs
ops_for_size() {
  local s="$1"
  case "$s" in
    8B|16B|32B|64B|128B|256B|512B|1KB|4KB|8KB|16KB|32KB|64KB)
      echo 10000
      ;;
    128KB|256KB|512KB|1MB|2MB|4MB|8MB)
      echo 3000
      ;;
    16MB|32MB|64MB)
      echo 1000
      ;;
    128MB)
      echo 256
      ;;
    *)
      echo 3000
      ;;
  esac
}

# Thread cap per size for m5d.2xlarge (8 vCPU)
threads_for_size() {
  local s="$1"
  case "$s" in
    8B|16B|32B|64B|128B|256B|512B|1KB|4KB|8KB|16KB|32KB|64KB)
      echo "1 2 4 8 16 32 64"          # small objs benefit from higher parallelism
      ;;
    128KB|256KB|512KB|1MB|2MB|4MB|8MB)
      echo "1 2 4 8 16 32"             # medium sizes: 32 is plenty for 8 vCPUs
      ;;
    16MB|32MB|64MB|128MB)
      echo "1 2 4 8 16"                 # large objs: bandwidth-bound; >16 wastes cycles
      ;;
    *)
      echo "1 2 4 8 16 32"
      ;;
  esac
}

BIN="./build/s3_bench"

mkdir -p "$RESULTS_DIR"

echo "Prefix: $PREFIX"
# echo "Preparing range object (2GB) once..."
# $BIN --op=write --bucket="$BUCKET" --region="$REGION" --prefix="$PREFIX" \
#      --size=2GB --threads=8 --total_ops=1 --single_key=range.bin --results_dir="$RESULTS_DIR"

for size in "${SIZES[@]}"; do
  AS=$(addr_space_for_size "$size")
  OPS=$(ops_for_size "$size")
  IFS=' ' read -r -a THREADS_EFF <<< "$(threads_for_size "$size")"

  echo "======== Size: $size | addr_space=$AS | ops=$OPS | threads_set=[${THREADS_EFF[*]}] ========"

  # 1) write-object
  for th in "${THREADS_EFF[@]}"; do
    for pat in "${PATTERNS[@]}"; do
      echo "[WRITE] size=$size threads=$th pattern=$pat"
      $BIN --op=write --bucket="$BUCKET" --region="$REGION" --prefix="$PREFIX" \
           --size="$size" --threads="$th" --total_ops="$OPS" \
           --pattern="$pat" --zipf_s="$ZIPF_S" --addr_space="$AS" \
           --results_dir="$RESULTS_DIR"
    done
  done

  # 2) read-object
  for th in "${THREADS_EFF[@]}"; do
    for pat in "${PATTERNS[@]}"; do
      echo "[READ-OBJECT] size=$size threads=$th pattern=$pat"
      $BIN --op=read-object --bucket="$BUCKET" --region="$REGION" --prefix="$PREFIX" \
           --size="$size" --threads="$th" --total_ops="$OPS" \
           --pattern="$pat" --zipf_s="$ZIPF_S" --addr_space="$AS" \
           --sink="$SINK" --results_dir="$RESULTS_DIR"
    done
  done

done

echo "All tests finished. Summary at $RESULTS_DIR/summary.csv"