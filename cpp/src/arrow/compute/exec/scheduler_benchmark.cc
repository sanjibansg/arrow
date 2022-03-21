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
  explicit Workload(int32_t size) : size_(size), data_(kDataSize) {
    std::default_random_engine gen(42);
    std::uniform_int_distribution<uint64_t> dist(0, std::numeric_limits<uint64_t>::max());
    std::generate(data_.begin(), data_.end(), [&]() { return dist(gen); });
  }

  void operator()();

 private:
  static constexpr int32_t kDataSize = 32;

  int32_t size_;
  std::vector<uint64_t> data_;
};

void Workload::operator()() {
  uint64_t result = 0;
  for (int32_t i = 0; i < size_ / kDataSize; ++i) {
    for (const auto v : data_) {
      result = (result << (v % 64)) - v;
    }
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
  const auto workload_size = static_cast<int32_t>(state.range(0));

  Workload workload(workload_size);

  for (auto _ : state) {
    workload();
    workload();
  }
}

static void BatchedExecution(benchmark::State& state) {  // NOLINT non-const reference
  const auto workload_size = static_cast<int32_t>(state.range(0));
  const auto batch_size = static_cast<int32_t>(state.range(1));
  Workload workload(batch_size);

  for (auto _ : state) {
    for(int i = 0; i < workload_size; i+= batch_size){
      workload();
      workload();
    }
  }
}

BENCHMARK(GroupedExecution)->Arg(1000000);
BENCHMARK(BatchedExecution)->Args({1000000,8000});
}
}