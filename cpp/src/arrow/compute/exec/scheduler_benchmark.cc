#include "benchmark/benchmark.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <vector>

#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace internal {

struct Workload {
  explicit Workload(int32_t size) : data_(size), size_(size), indices_(size) {
    std::default_random_engine gen(42);
    std::uniform_int_distribution<uint64_t> dist(0, std::numeric_limits<uint64_t>::max());
    std::generate(data_.begin(), data_.end(), [&]() { return dist(gen); });
    
    std::iota(indices_.begin(), indices_.end(), 0);
    std::shuffle(indices_.begin(), indices_.end(), std::default_random_engine(42));  
  }

  void operator()();

 private:
  std::vector<uint64_t> data_;
  uint64_t size_;
  std::vector<uint64_t> indices_;
};

void Workload::operator()() {
  uint64_t result = 0;
  for (uint64_t i = 0; i<size_; ++i) {
    result = (result << (data_[indices_[i]] % 64)) - data_[indices_[i]];
  }
  benchmark::DoNotOptimize(result);
}

struct Task {
  explicit Task(int32_t size) : workload_(size) {}

  Status operator()() {
    workload_();
    return Status::OK();
  }

 private:
  Workload workload_;
};

static void GroupedExecution(benchmark::State& state) {  // NOLINT non-const reference
  const auto workload_size = static_cast<int64_t>(state.range(0));

  Workload workload(workload_size);

  for (auto _ : state) {
    workload();
    workload();
  }
}

static void BatchedExecution(benchmark::State& state) {  // NOLINT non-const reference
  const auto workload_size = static_cast<int64_t>(state.range(0));
  const auto batch_size = 8000;
  Workload workload(batch_size);

  for (auto _ : state) {
    for (int i = 0; i < workload_size; i += batch_size) {
      workload();
      workload();
    }
  }
}

BENCHMARK(GroupedExecution)->RangeMultiplier(2)->Range(2<<10, 2<<29);
BENCHMARK(BatchedExecution)->RangeMultiplier(2)->Range(2<<10, 2<<29);
}  // namespace internal
}  // namespace arrow
