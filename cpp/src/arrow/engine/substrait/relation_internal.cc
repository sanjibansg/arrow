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

#include "arrow/engine/substrait/relation_internal.h"

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/exec/options.h"
#include "arrow/csv/options.h"
#include "arrow/dataset/file_csv.h"
#include "arrow/dataset/file_ipc.h"
#include "arrow/dataset/file_parquet.h"
#include "arrow/dataset/plan.h"
#include "arrow/dataset/scanner.h"
#include "arrow/engine/substrait/expression_internal.h"
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/filesystem/localfs.h"

namespace arrow {
namespace engine {

template <typename RelMessage>
Status CheckRelCommon(const RelMessage& rel) {
  if (rel.has_common()) {
    if (rel.common().has_emit()) {
      return Status::NotImplemented("substrait::RelCommon::Emit");
    }
    if (rel.common().has_hint()) {
      return Status::NotImplemented("substrait::RelCommon::Hint");
    }
    if (rel.common().has_advanced_extension()) {
      return Status::NotImplemented("substrait::RelCommon::advanced_extension");
    }
  }
  if (rel.has_advanced_extension()) {
    return Status::NotImplemented("substrait AdvancedExtensions");
  }
  return Status::OK();
}

Result<compute::Declaration> FromProto(const substrait::Rel& rel,
                                       const ExtensionSet& ext_set) {
  static bool dataset_init = false;
  if (!dataset_init) {
    dataset_init = true;
    dataset::internal::Initialize();
  }

  switch (rel.rel_type_case()) {
    case substrait::Rel::RelTypeCase::kRead: {
      const auto& read = rel.read();
      RETURN_NOT_OK(CheckRelCommon(read));

      ARROW_ASSIGN_OR_RAISE(auto base_schema, FromProto(read.base_schema(), ext_set));

      auto scan_options = std::make_shared<dataset::ScanOptions>();

      if (read.has_filter()) {
        ARROW_ASSIGN_OR_RAISE(scan_options->filter, FromProto(read.filter(), ext_set));
      }

      if (read.has_projection()) {
        // NOTE: scan_options->projection is not used by the scanner and thus can't be
        // used for this
        return Status::NotImplemented("substrait::ReadRel::projection");
      }

      if (!read.has_local_files()) {
        return Status::NotImplemented(
            "substrait::ReadRel with read_type other than LocalFiles");
      }

      if (read.local_files().has_advanced_extension()) {
        return Status::NotImplemented(
            "substrait::ReadRel::LocalFiles::advanced_extension");
      }

      std::shared_ptr<dataset::FileFormat> format;
      auto filesystem = std::make_shared<fs::LocalFileSystem>();
      std::vector<std::shared_ptr<dataset::FileFragment>> fragments;

      for (const auto& item : read.local_files().items()) {
        if (item.path_type_case() !=
            substrait::ReadRel_LocalFiles_FileOrFiles::kUriFile) {
          return Status::NotImplemented(
              "substrait::ReadRel::LocalFiles::FileOrFiles with "
              "path_type other than uri_file");
        }
        
        if (item.has_csv_options()){
              std::shared_ptr<dataset::CsvFileFormat> csv_format;
              csv::ParseOptions parse_options;
              parse_options.delimiter = item.csv_options().parse_options().delimiter().front();
              parse_options.quoting = item.csv_options().parse_options().quoting();
              parse_options.quote_char = item.csv_options().parse_options().quote_char().front();
              parse_options.double_quote = item.csv_options().parse_options().double_quote();
              parse_options.escaping = item.csv_options().parse_options().escaping();
              parse_options.escape_char = item.csv_options().parse_options().escape_char().front();
              parse_options.newlines_in_values = item.csv_options().parse_options().newlines_in_values();
              parse_options.ignore_empty_lines = item.csv_options().parse_options().ignore_empty_lines();

              csv_format->parse_options = parse_options;
              format = std::dynamic_pointer_cast<dataset::FileFormat>(csv_format);

              std::shared_ptr<dataset::CsvFragmentScanOptions> fragment_csv_scan_options;
              fragment_csv_scan_options->read_options.use_threads = !(item.csv_options().read_options().no_use_threads());
              fragment_csv_scan_options->read_options.block_size = item.csv_options().read_options().block_size();
              fragment_csv_scan_options->read_options.skip_rows = item.csv_options().read_options().skip_rows();
              fragment_csv_scan_options->read_options.skip_rows_after_names = item.csv_options().read_options().skip_rows_after_names();
              for(const auto& column_name : item.csv_options().read_options().column_names()){
                fragment_csv_scan_options->read_options.column_names.emplace_back(column_name);
              }
              fragment_csv_scan_options->read_options.autogenerate_column_names = item.csv_options().read_options().autogenerate_column_names();

              fragment_csv_scan_options->convert_options.check_utf8 = !(item.csv_options().convert_options().ignore_check_utf8());
              for(const auto& null_value : item.csv_options().convert_options().null_values()){
                fragment_csv_scan_options->convert_options.null_values.emplace_back(null_value);
              }
              for(const auto& true_value : item.csv_options().convert_options().true_values()){
                fragment_csv_scan_options->convert_options.true_values.emplace_back(true_value);
              }
              for(const auto& false_value : item.csv_options().convert_options().false_values()){
                fragment_csv_scan_options->convert_options.false_values.emplace_back(false_value);
              }              
              fragment_csv_scan_options->convert_options.strings_can_be_null = item.csv_options().convert_options().strings_can_be_null();
              fragment_csv_scan_options->convert_options.quoted_strings_can_be_null = !(item.csv_options().convert_options().quoted_strings_cannot_be_null());
              fragment_csv_scan_options->convert_options.auto_dict_encode = item.csv_options().convert_options().auto_dict_encode();
              fragment_csv_scan_options->convert_options.auto_dict_max_cardinality = item.csv_options().convert_options().auto_dict_max_cardinality();
              fragment_csv_scan_options->convert_options.decimal_point = item.csv_options().convert_options().decimal_point().front();
              for(const auto& include_column : item.csv_options().convert_options().include_columns()){
                fragment_csv_scan_options->convert_options.include_columns.emplace_back(include_column);
              }   
              fragment_csv_scan_options->convert_options.include_missing_columns = item.csv_options().convert_options().include_missing_columns();
              scan_options->fragment_scan_options = std::dynamic_pointer_cast<dataset::FragmentScanOptions>(fragment_csv_scan_options);

        } else {
          if (item.format() ==
              substrait::ReadRel::LocalFiles::FileOrFiles::FILE_FORMAT_PARQUET) {
            format = std::make_shared<dataset::ParquetFileFormat>();
          } else if (util::string_view{item.uri_file()}.ends_with(".arrow")) {
            format = std::make_shared<dataset::IpcFileFormat>();
          } else if (util::string_view{item.uri_file()}.ends_with(".feather")) {
            format = std::make_shared<dataset::IpcFileFormat>();
          } else {
            return Status::NotImplemented(
                "substrait::ReadRel::LocalFiles::FileOrFiles::format "
                "other than FILE_FORMAT_PARQUET");
          }
      }

        if (!util::string_view{item.uri_file()}.starts_with("file:///")) {
          return Status::NotImplemented(
              "substrait::ReadRel::LocalFiles::FileOrFiles::uri_file "
              "with other than local filesystem (file:///)");
        }
        auto path = item.uri_file().substr(7);

        if (item.partition_index() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::partition_index");
        }

        if (item.start() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::start offset");
        }

        if (item.length() != 0) {
          return Status::NotImplemented(
              "non-default substrait::ReadRel::LocalFiles::FileOrFiles::length");
        }

        ARROW_ASSIGN_OR_RAISE(auto fragment, format->MakeFragment(dataset::FileSource{
                                                 std::move(path), filesystem}));
        fragments.push_back(std::move(fragment));
      }

      ARROW_ASSIGN_OR_RAISE(
          auto ds, dataset::FileSystemDataset::Make(
                       std::move(base_schema), /*root_partition=*/compute::literal(true),
                       std::move(format), std::move(filesystem), std::move(fragments)));

      return compute::Declaration{
          "scan", dataset::ScanNodeOptions{std::move(ds), std::move(scan_options)}};
    }

    case substrait::Rel::RelTypeCase::kFilter: {
      const auto& filter = rel.filter();
      RETURN_NOT_OK(CheckRelCommon(filter));

      if (!filter.has_input()) {
        return Status::Invalid("substrait::FilterRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProto(filter.input(), ext_set));

      if (!filter.has_condition()) {
        return Status::Invalid("substrait::FilterRel with no condition expression");
      }
      ARROW_ASSIGN_OR_RAISE(auto condition, FromProto(filter.condition(), ext_set));

      return compute::Declaration::Sequence({
          std::move(input),
          {"filter", compute::FilterNodeOptions{std::move(condition)}},
      });
    }

    case substrait::Rel::RelTypeCase::kProject: {
      const auto& project = rel.project();
      RETURN_NOT_OK(CheckRelCommon(project));

      if (!project.has_input()) {
        return Status::Invalid("substrait::ProjectRel with no input relation");
      }
      ARROW_ASSIGN_OR_RAISE(auto input, FromProto(project.input(), ext_set));

      std::vector<compute::Expression> expressions;
      for (const auto& expr : project.expressions()) {
        expressions.emplace_back();
        ARROW_ASSIGN_OR_RAISE(expressions.back(), FromProto(expr, ext_set));
      }

      return compute::Declaration::Sequence({
          std::move(input),
          {"project", compute::ProjectNodeOptions{std::move(expressions)}},
      });
    }

    default:
      break;
  }

  return Status::NotImplemented(
      "conversion to arrow::compute::Declaration from Substrait relation ",
      rel.DebugString());
}

}  // namespace engine
}  // namespace arrow
