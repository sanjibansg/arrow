// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <mutex>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/async_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/tracing_internal.h"
#include "arrow/util/unreachable.h"
#include "arrow/util/vector.h"

namespace arrow {

using internal::checked_cast;
using internal::MapVector;

namespace compute {
namespace {

struct MorselBatchPipelineTask {
  void operator()() {
    auto Partition = [](const ExecBatch& batch, int64_t batch_size) {
      int64_t batch_index = 0;
      std::vector<ExecBatch> batches;
      while (batch_index < batch.length) {
        batches.push_back(batch.Slice(batch_index, batch_index + batch_size));
        batch_index += batch_size;
      }
      return batches;
    };
    for (const auto& batch : Partition(morsel, batch_size)) {
      start_node->InputReceived(target_node, batch);
    }
  }
  ExecNode* start_node;
  ExecNode* target_node;
  ExecBatch morsel;
  int64_t batch_size;
};

struct SourceNode : ExecNode {
  SourceNode(ExecPlan* plan, std::shared_ptr<Schema> output_schema,
             AsyncGenerator<util::optional<ExecBatch>> generator,
             int64_t batch_size = 32768)
      : ExecNode(plan, {}, {}, std::move(output_schema),
                 /*num_outputs=*/1),
        generator_(std::move(generator)),
        batch_size(batch_size) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 0, "SourceNode"));
    const auto& source_options = checked_cast<const SourceNodeOptions&>(options);
    return plan->EmplaceNode<SourceNode>(plan, source_options.output_schema,
                                         source_options.generator,
                                         source_options.batch_size);
  }

  const char* kind_name() const override { return "SourceNode"; }

  [[noreturn]] static void NoInputs() {
    Unreachable("no inputs; this should never be called");
  }
  [[noreturn]] void InputReceived(ExecNode*, ExecBatch) override { NoInputs(); }
  [[noreturn]] void ErrorReceived(ExecNode*, Status) override { NoInputs(); }
  [[noreturn]] void InputFinished(ExecNode*, int) override { NoInputs(); }

  Status StartProducing() override {
    START_SPAN(span_, std::string(kind_name()) + ":" + label(),
               {{"node.kind", kind_name()},
                {"node.label", label()},
                {"node.output_schema", output_schema()->ToString()},
                {"node.detail", ToString()}});
    {
      // If another exec node encountered an error during its StartProducing call
      // it might have already called StopProducing on all of its inputs (including this
      // node).
      //
      std::unique_lock<std::mutex> lock(mutex_);
      if (stop_requested_) {
        return Status::OK();
      }
    }

    CallbackOptions options;
    auto executor = plan()->exec_context()->executor();
    if (executor) {
      // These options will transfer execution to the desired Executor if necessary.
      // This can happen for in-memory scans where batches didn't require
      // any CPU work to decode. Otherwise, parsing etc should have already
      // been placed us on the desired Executor and no queues will be pushed to.
      options.executor = executor;
      options.should_schedule = ShouldSchedule::IfDifferentExecutor;
    }
    finished_ =
        Loop([this, executor, options] {
          std::unique_lock<std::mutex> lock(mutex_);
          int total_batches = batch_count_++;
          if (stop_requested_) {
            return Future<ControlFlow<int>>::MakeFinished(Break(total_batches));
          }
          lock.unlock();

          return generator_().Then(
              [=](const util::optional<ExecBatch>& maybe_batch) -> ControlFlow<int> {
                std::unique_lock<std::mutex> lock(mutex_);
                if (IsIterationEnd(maybe_batch) || stop_requested_) {
                  stop_requested_ = true;
                  return Break(total_batches);
                }
                lock.unlock();
                ExecBatch batch = std::move(*maybe_batch);

                if (executor) {
                  auto status =
                      task_group_.AddTask([this, executor, batch]() -> Result<Future<>> {
                        return executor->Submit(MorselBatchPipelineTask{
                            outputs_[0], this, batch, batch_size});
                      });
                  if (!status.ok()) {
                    outputs_[0]->ErrorReceived(this, std::move(status));
                    return Break(total_batches);
                  }
                } else {
                  MorselBatchPipelineTask{outputs_[0], this, batch, batch_size}();
                }
                return Continue();
              },
              [=](const Status& error) -> ControlFlow<int> {
                // NB: ErrorReceived is independent of InputFinished, but
                // ErrorReceived will usually prompt StopProducing which will
                // prompt InputFinished. ErrorReceived may still be called from a
                // node which was requested to stop (indeed, the request to stop
                // may prompt an error).
                std::unique_lock<std::mutex> lock(mutex_);
                stop_requested_ = true;
                lock.unlock();
                outputs_[0]->ErrorReceived(this, error);
                return Break(total_batches);
              },
              options);
        }).Then([&](int total_batches) {
          outputs_[0]->InputFinished(this, total_batches);
          return task_group_.End();
        });
    END_SPAN_ON_FUTURE_COMPLETION(span_, finished_, this);
    return Status::OK();
  }

  void PauseProducing(ExecNode* output) override {}

  void ResumeProducing(ExecNode* output) override {}

  void StopProducing(ExecNode* output) override {
    DCHECK_EQ(output, outputs_[0]);
    StopProducing();
  }

  void StopProducing() override {
    std::unique_lock<std::mutex> lock(mutex_);
    stop_requested_ = true;
  }

  Future<> finished() override { return finished_; }

 private:
  std::mutex mutex_;
  bool stop_requested_{false};
  int batch_count_{0};
  util::AsyncTaskGroup task_group_;
  AsyncGenerator<util::optional<ExecBatch>> generator_;
  int64_t batch_size;
};

struct TableSourceNode : public SourceNode {
  TableSourceNode(ExecPlan* plan, std::shared_ptr<Table> table, int64_t batch_size,
                  int64_t partition_batch_size)
      : SourceNode(plan, table->schema(), TableGenerator(*table, batch_size),
                   partition_batch_size) {}

  static Result<ExecNode*> Make(ExecPlan* plan, std::vector<ExecNode*> inputs,
                                const ExecNodeOptions& options) {
    RETURN_NOT_OK(ValidateExecNodeInputs(plan, inputs, 0, "TableSourceNode"));
    const auto& table_options = checked_cast<const TableSourceNodeOptions&>(options);
    const auto& table = table_options.table;
    const int64_t batch_size = table_options.max_batch_size;
    const int64_t partition_batch_size = table_options.partition_batch_size;

    RETURN_NOT_OK(ValidateTableSourceNodeInput(table, batch_size));

    return plan->EmplaceNode<TableSourceNode>(plan, table, batch_size,
                                              partition_batch_size);
  }

  const char* kind_name() const override { return "TableSourceNode"; }

  static arrow::Status ValidateTableSourceNodeInput(const std::shared_ptr<Table> table,
                                                    const int64_t batch_size) {
    if (table == nullptr) {
      return Status::Invalid("TableSourceNode node requires table which is not null");
    }

    if (batch_size <= 0) {
      return Status::Invalid(
          "TableSourceNode node requires, batch_size > 0 , but got batch size ",
          batch_size);
    }

    return Status::OK();
  }

  static arrow::AsyncGenerator<util::optional<ExecBatch>> TableGenerator(
      const Table& table, const int64_t batch_size) {
    auto batches = ConvertTableToExecBatches(table, batch_size);
    auto opt_batches =
        MapVector([](ExecBatch batch) { return util::make_optional(std::move(batch)); },
                  std::move(batches));
    AsyncGenerator<util::optional<ExecBatch>> gen;
    gen = MakeVectorGenerator(std::move(opt_batches));
    return gen;
  }

  static std::vector<ExecBatch> ConvertTableToExecBatches(const Table& table,
                                                          const int64_t batch_size) {
    std::shared_ptr<TableBatchReader> reader = std::make_shared<TableBatchReader>(table);

    // setting chunksize for the batch reader
    reader->set_chunksize(batch_size);

    std::shared_ptr<RecordBatch> batch;
    std::vector<ExecBatch> exec_batches;
    while (true) {
      auto batch_res = reader->Next();
      if (batch_res.ok()) {
        batch = std::move(batch_res).MoveValueUnsafe();
      }
      if (batch == NULLPTR) {
        break;
      }
      exec_batches.emplace_back(*batch);
    }
    return exec_batches;
  }
};

}  // namespace

namespace internal {

void RegisterSourceNode(ExecFactoryRegistry* registry) {
  DCHECK_OK(registry->AddFactory("source", SourceNode::Make));
  DCHECK_OK(registry->AddFactory("table_source", TableSourceNode::Make));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
