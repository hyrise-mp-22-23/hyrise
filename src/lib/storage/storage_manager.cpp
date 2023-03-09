#include "storage_manager.hpp"

#include <sys/fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fstream>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "import_export/binary/binary_writer.hpp"
#include "import_export/file_type.hpp"
#include "operators/export.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/job_task.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/base_dictionary_segment.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/dictionary_segment/dictionary_segment_iterable.hpp"
#include "storage/value_segment.hpp"
#include "utils/assert.hpp"
#include "utils/meta_table_manager.hpp"
using json = nlohmann::json;

namespace {

using namespace hyrise;  // NOLINT

uint32_t element_index(const uint32_t byte_index, const size_t element_size) {
  return byte_index / element_size;
}

/*
 * Copied binary writing function from `binary_writer.cpp`
 */

template <typename T>
void export_value(const T& value, std::string file_name) {
  std::ofstream ofstream(file_name, std::ios::binary | std::ios::app);
  ofstream.write(reinterpret_cast<const char*>(&value), sizeof(T));
  ofstream.close();
}

// not copied, own creation
void overwrite_header(const FILE_HEADER header, std::string file_name) {
  //yes, all modes are needed exactly like that to allow us to overwrite the first part of the file
  //yes, fstream should open with "std::ios::in | std::ios::out by default", don't ask me, this was a pain to figure out
  std::fstream fstream(file_name, std::ios::binary | std::ios::in | std::ios::out);
  fstream.seekp(0, std::ios_base::beg);
  fstream.write(reinterpret_cast<const char*>(&header), sizeof(FILE_HEADER));
  fstream.close();
}

template <typename T, typename Alloc>
void export_values(const std::vector<T, Alloc>& values, std::string file_name) {
  std::ofstream ofstream(file_name, std::ios::binary | std::ios::app);
  ofstream.write(reinterpret_cast<const char*>(values.data()), values.size() * sizeof(T));
  ofstream.close();
}

template <typename T>
void export_values(const std::span<const T>& data_span, std::string file_name) {
  std::ofstream ofstream(file_name, std::ios::binary | std::ios::app);
  ofstream.write(reinterpret_cast<const char*>(data_span.data()), data_span.size() * sizeof(T));
  ofstream.close();
}

void export_values(const FixedStringSpan& data_span, std::string file_name) {
  std::ofstream ofstream(file_name, std::ios::binary | std::ios::app);
  ofstream.write(reinterpret_cast<const char*>(data_span.data()), data_span.size() * data_span.string_length());
  ofstream.close();
}

// needed for attribute vector which is stored in a compact manner
void export_compact_vector(const pmr_compact_vector& values, std::string file_name) {
  export_value(values.bits(), file_name);
  std::ofstream ofstream(file_name, std::ios::binary | std::ios::app);
  ofstream.write(reinterpret_cast<const char*>(values.get()), static_cast<int64_t>(values.bytes()));
  ofstream.close();
}

void export_compressed_vector(const CompressedVectorType type, const BaseCompressedVector& compressed_vector,
                              std::string file_name) {
  switch (type) {
    case CompressedVectorType::FixedWidthInteger4Byte:
      export_values(dynamic_cast<const FixedWidthIntegerVector<uint32_t>&>(compressed_vector).data(), file_name);
      return;
    case CompressedVectorType::FixedWidthInteger2Byte:
      export_values(dynamic_cast<const FixedWidthIntegerVector<uint16_t>&>(compressed_vector).data(), file_name);
      return;
    case CompressedVectorType::FixedWidthInteger1Byte:
      export_values(dynamic_cast<const FixedWidthIntegerVector<uint8_t>&>(compressed_vector).data(), file_name);
      return;
    case CompressedVectorType::BitPacking:
      export_compact_vector(dynamic_cast<const BitPackingVector&>(compressed_vector).data(), file_name);
      return;
    default:
      Fail("Any other type should have been caught before.");
  }
}

uint32_t calculate_byte_size_of_attribute_vector(std::shared_ptr<const BaseCompressedVector> attribute_vector) {
  const auto compressed_vector_type = attribute_vector->type();
  auto size = uint32_t{};
  switch (compressed_vector_type) {
    case CompressedVectorType::FixedWidthInteger1Byte:
      size = attribute_vector->size();
      break;
    case CompressedVectorType::FixedWidthInteger2Byte:
      size = attribute_vector->size() * 2;
      break;
    case CompressedVectorType::FixedWidthInteger4Byte:
      size = attribute_vector->size() * 4;
      break;
    case CompressedVectorType::BitPacking:
      size += 4;
      size += dynamic_cast<const BitPackingVector&>(*attribute_vector).data().bytes();
      break;
    default:
      Fail("Unknown Compression Type in Storage Manager.");
  }
  return size;
}

[[maybe_unused]] void evaluate_mapped_chunk(const std::shared_ptr<Chunk>& chunk,
                                            const std::shared_ptr<Chunk>& mapped_chunk) {
  const auto created_segment = chunk->get_segment(ColumnID{0});

  resolve_data_type(created_segment->data_type(), [&](auto segment_data_type) {
    using SegmentDataType = typename decltype(segment_data_type)::type;
    const auto created_dict_segment = dynamic_pointer_cast<DictionarySegment<SegmentDataType>>(created_segment);
    auto dict_segment_iterable = create_iterable_from_segment<SegmentDataType>(*created_dict_segment);

    auto column_sum_of_created_chunk = SegmentDataType{};
    dict_segment_iterable.with_iterators([&](auto it, auto end) {
      column_sum_of_created_chunk =
          std::accumulate(it, end, SegmentDataType{0}, [](const auto& accumulator, const auto& currentValue) {
            return accumulator + SegmentDataType{currentValue.value()};
          });
    });

    std::cout << "Sum of column 1 of created chunk: " << column_sum_of_created_chunk << std::endl;
  });

  const auto mapped_segment = mapped_chunk->get_segment(ColumnID{0});

  resolve_data_type(mapped_segment->data_type(), [&](auto segment_data_type) {
    using SegmentDataType = typename decltype(segment_data_type)::type;
    const auto mapped_dict_segment = dynamic_pointer_cast<DictionarySegment<SegmentDataType>>(mapped_segment);
    auto mapped_dict_segment_iterable = create_iterable_from_segment<SegmentDataType>(*mapped_dict_segment);

    auto column_sum_of_mapped_chunk = SegmentDataType{};
    mapped_dict_segment_iterable.with_iterators([&](auto it, auto end) {
      column_sum_of_mapped_chunk =
          std::accumulate(it, end, SegmentDataType{0}, [](const auto& accumulator, const auto& currentValue) {
            return accumulator + SegmentDataType{currentValue.value()};
          });
    });

    std::cout << "Sum of column 1 of mapped chunk: " << column_sum_of_mapped_chunk << std::endl;
  });

  // print row 17 of created and mapped chunk
  std::cout << "Row 2 of created chunk: ";
  for (auto column_index = ColumnID{0}; column_index < chunk->column_count(); ++column_index) {
    const auto segment = chunk->get_segment(column_index);
    const auto attribute_value = (*segment)[ChunkOffset{1}];
    std::cout << attribute_value << " ";
  }
  std::cout << std::endl;

  std::cout << "Row 2 of mapped chunk: ";
  for (auto column_index = ColumnID{0}; column_index < mapped_chunk->column_count(); ++column_index) {
    const auto segment = mapped_chunk->get_segment(column_index);
    const auto attribute_value = (*segment)[ChunkOffset{1}];
    std::cout << attribute_value << " ";
  }
  std::cout << std::endl;
}

}  // namespace

namespace hyrise {

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  const auto table_iter = _tables.find(name);
  const auto view_iter = _views.find(name);
  Assert(table_iter == _tables.end() || !table_iter->second,
         "Cannot add table " + name + " - a table with the same name already exists");
  Assert(view_iter == _views.end() || !view_iter->second,
         "Cannot add table " + name + " - a view with the same name already exists");

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
    // We currently assume that all tables stored in the StorageManager are mutable and, as such, have MVCC data. This
    // way, we do not need to check query plans if they try to update immutable tables. However, this is not a hard
    // limitation and might be changed into more fine-grained assertions if the need arises.
    Assert(table->get_chunk(chunk_id)->has_mvcc_data(), "Table must have MVCC data.");
  }

  // Create table statistics and chunk pruning statistics for added table.

  table->set_table_statistics(TableStatistics::from_table(*table));
  generate_chunk_pruning_statistics(table);

  _tables[name] = std::move(table);

  auto table_persistence_file_name = name + "_0.bin";
  _tables_current_persistence_file_mapping[name] = {table_persistence_file_name, 0, 0};
}

void StorageManager::drop_table(const std::string& name) {
  const auto table_iter = _tables.find(name);
  Assert(table_iter != _tables.end() && table_iter->second, "Error deleting table. No such table named '" + name + "'");

  // The concurrent_unordered_map does not support concurrency-safe erasure. Thus, we simply reset the table pointer.
  _tables[name] = nullptr;
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  const auto table_iter = _tables.find(name);
  Assert(table_iter != _tables.end(), "No such table named '" + name + "'");

  auto table = table_iter->second;
  Assert(table,
         "Nullptr found when accessing table named '" + name + "'. This can happen if a dropped table is accessed.");

  return table;
}

std::string StorageManager::_get_table_name(const Table* address) const {
  for (const auto& [name, pointer] : _tables) {
    if (pointer.get() == address) {
      return name;
    }
  }
  return std::string{};
}

bool StorageManager::has_table(const std::string& name) const {
  const auto table_iter = _tables.find(name);
  return table_iter != _tables.end() && table_iter->second;
}

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> table_names;
  table_names.reserve(_tables.size());

  for (const auto& table_item : _tables) {
    if (!table_item.second) {
      continue;
    }

    table_names.emplace_back(table_item.first);
  }

  return table_names;
}

std::unordered_map<std::string, std::shared_ptr<Table>> StorageManager::tables() const {
  std::unordered_map<std::string, std::shared_ptr<Table>> result;

  for (const auto& [table_name, table] : _tables) {
    // Skip dropped table, as we don't remove the map entry when dropping, but only reset the table pointer.
    if (!table) {
      continue;
    }

    result[table_name] = table;
  }

  return result;
}

void StorageManager::add_view(const std::string& name, const std::shared_ptr<LQPView>& view) {
  const auto table_iter = _tables.find(name);
  const auto view_iter = _views.find(name);
  Assert(table_iter == _tables.end() || !table_iter->second,
         "Cannot add view " + name + " - a table with the same name already exists");
  Assert(view_iter == _views.end() || !view_iter->second,
         "Cannot add view " + name + " - a view with the same name already exists");

  _views[name] = view;
}

void StorageManager::drop_view(const std::string& name) {
  const auto view_iter = _views.find(name);
  Assert(view_iter != _views.end() && view_iter->second, "Error deleting view. No such view named '" + name + "'");

  _views[name] = nullptr;
}

std::shared_ptr<LQPView> StorageManager::get_view(const std::string& name) const {
  const auto view_iter = _views.find(name);
  Assert(view_iter != _views.end(), "No such view named '" + name + "'");

  const auto view = view_iter->second;
  Assert(view,
         "Nullptr found when accessing view named '" + name + "'. This can happen if a dropped view is accessed.");

  return view->deep_copy();
}

bool StorageManager::has_view(const std::string& name) const {
  const auto view_iter = _views.find(name);
  return view_iter != _views.end() && view_iter->second;
}

std::vector<std::string> StorageManager::view_names() const {
  std::vector<std::string> view_names;
  view_names.reserve(_views.size());

  for (const auto& view_item : _views) {
    if (!view_item.second) {
      continue;
    }

    view_names.emplace_back(view_item.first);
  }

  return view_names;
}

std::unordered_map<std::string, std::shared_ptr<LQPView>> StorageManager::views() const {
  std::unordered_map<std::string, std::shared_ptr<LQPView>> result;

  for (const auto& [view_name, view] : _views) {
    if (!view) {
      continue;
    }

    result[view_name] = view;
  }

  return result;
}

void StorageManager::add_prepared_plan(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan) {
  const auto iter = _prepared_plans.find(name);
  Assert(iter == _prepared_plans.end() || !iter->second,
         "Cannot add prepared plan " + name + " - a prepared plan with the same name already exists");

  _prepared_plans[name] = prepared_plan;
}

std::shared_ptr<PreparedPlan> StorageManager::get_prepared_plan(const std::string& name) const {
  const auto iter = _prepared_plans.find(name);
  Assert(iter != _prepared_plans.end(), "No such prepared plan named '" + name + "'");

  auto prepared_plan = iter->second;
  Assert(prepared_plan, "Nullptr found when accessing prepared plan named '" + name +
                            "'. This can happen if a dropped prepared plan is accessed.");

  return prepared_plan;
}

bool StorageManager::has_prepared_plan(const std::string& name) const {
  const auto iter = _prepared_plans.find(name);
  return iter != _prepared_plans.end() && iter->second;
}

void StorageManager::drop_prepared_plan(const std::string& name) {
  const auto iter = _prepared_plans.find(name);
  Assert(iter != _prepared_plans.end() && iter->second,
         "Error deleting prepared plan. No such prepared plan named '" + name + "'");

  _prepared_plans[name] = nullptr;
}

std::unordered_map<std::string, std::shared_ptr<PreparedPlan>> StorageManager::prepared_plans() const {
  std::unordered_map<std::string, std::shared_ptr<PreparedPlan>> result;

  for (const auto& [prepared_plan_name, prepared_plan] : _prepared_plans) {
    if (!prepared_plan) {
      continue;
    }

    result[prepared_plan_name] = prepared_plan;
  }

  return result;
}

void StorageManager::export_all_tables_as_csv(const std::string& path) {
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  tasks.reserve(_tables.size());

  for (const auto& table_item : _tables) {
    if (!table_item.second) {
      continue;
    }

    auto job_task = std::make_shared<JobTask>([table_item, &path]() {
      const auto& name = table_item.first;
      const auto& table = table_item.second;

      auto table_wrapper = std::make_shared<TableWrapper>(table);
      table_wrapper->execute();

      auto export_csv = std::make_shared<Export>(table_wrapper, path + "/" + name + ".csv", FileType::Csv);  // NOLINT
      export_csv->execute();
    });
    tasks.push_back(job_task);
    job_task->schedule();
  }

  Hyrise::get().scheduler()->wait_for_tasks(tasks);
}

std::ostream& operator<<(std::ostream& stream, const StorageManager& storage_manager) {
  stream << "==================" << std::endl;
  stream << "===== Tables =====" << std::endl << std::endl;

  for (auto const& table : storage_manager.tables()) {
    stream << "==== table >> " << table.first << " <<";
    stream << " (" << table.second->column_count() << " columns, " << table.second->row_count() << " rows in "
           << table.second->chunk_count() << " chunks)";
    stream << std::endl;
  }

  stream << "==================" << std::endl;
  stream << "===== Views ======" << std::endl << std::endl;

  for (auto const& view : storage_manager.views()) {
    stream << "==== view >> " << view.first << " <<";
    stream << std::endl;
  }

  stream << "==================" << std::endl;
  stream << "= PreparedPlans ==" << std::endl << std::endl;

  for (auto const& prepared_plan : storage_manager.prepared_plans()) {
    stream << "==== prepared plan >> " << prepared_plan.first << " <<";
    stream << std::endl;
  }

  return stream;
}

std::vector<uint32_t> StorageManager::_calculate_segment_offset_ends(const std::shared_ptr<Chunk> chunk) const {
  const auto segment_count = chunk->column_count();
  auto segment_offset_ends = std::vector<uint32_t>(segment_count);

  auto offset_end = _chunk_header_bytes(segment_count);
  for (auto segment_index = size_t{0}; segment_index < segment_count; ++segment_index) {
    const auto abstract_segment = chunk->get_segment(static_cast<ColumnID>(static_cast<uint16_t>(segment_index)));

    resolve_data_type(abstract_segment->data_type(), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      if constexpr (std::is_same<ColumnDataType, pmr_string>::value) {
        offset_end += _segment_header_bytes + 4;
        //TODO: this should be encapsulated in a size_bytes() function
        const auto fixed_string_dict_segment =
            std::dynamic_pointer_cast<FixedStringDictionarySegment<ColumnDataType>>(abstract_segment);
        Assert(fixed_string_dict_segment, "Trying to map a non-FixedString String DictionarySegment");
        const auto fixed_string_dict_size_bytes = fixed_string_dict_segment->fixed_string_dictionary()->size() *
                                                  fixed_string_dict_segment->fixed_string_dictionary()->string_length();
        const auto attribute_vector_size_bytes =
            calculate_byte_size_of_attribute_vector(fixed_string_dict_segment->attribute_vector());
        offset_end += fixed_string_dict_size_bytes + attribute_vector_size_bytes;
      } else {
        offset_end += _segment_header_bytes;
        const auto dict_segment = dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(abstract_segment);
        const auto dictionary_size_bytes = dict_segment->dictionary()->size() * sizeof(ColumnDataType);
        const auto attribute_vector_size = calculate_byte_size_of_attribute_vector(dict_segment->attribute_vector());
        offset_end += dictionary_size_bytes + attribute_vector_size;
      }
      segment_offset_ends[segment_index] = offset_end;
    });
  }

  return segment_offset_ends;
}

template <typename T>
void StorageManager::_write_fixed_string_dict_segment_to_disk(
    const std::shared_ptr<FixedStringDictionarySegment<T>> segment, const std::string& file_name) const {
  const auto compressed_vector_type_id =
      _resolve_persisted_segment_encoding_type_from_compression_type(segment->compressed_vector_type().value());
  export_value(static_cast<uint32_t>(compressed_vector_type_id), file_name);
  export_value(static_cast<uint32_t>(segment->fixed_string_dictionary()->string_length()), file_name);
  export_value(static_cast<uint32_t>(segment->fixed_string_dictionary()->size()), file_name);
  export_value(static_cast<uint32_t>(segment->attribute_vector()->size()), file_name);

  export_values(*segment->fixed_string_dictionary(), file_name);
  export_compressed_vector(*segment->compressed_vector_type(), *segment->attribute_vector(), file_name);
  //TODO: What to do with non-compressed AttributeVectors?
}

template <typename T>
void StorageManager::_write_dict_segment_to_disk(const std::shared_ptr<DictionarySegment<T>> segment,
                                                 const std::string& file_path) const {
  /*
   * For a description of how dictionary segments look, see the following PR:
   *    https://github.com/hyrise-mp-22-23/hyrise/pull/94
   */
  const auto compressed_vector_type_id =
      _resolve_persisted_segment_encoding_type_from_compression_type(segment->compressed_vector_type().value());
  export_value(static_cast<uint32_t>(compressed_vector_type_id), file_path);
  export_value(static_cast<uint32_t>(segment->dictionary()->size()), file_path);
  export_value(static_cast<uint32_t>(segment->attribute_vector()->size()), file_path);

  // we need to ensure that every part can be mapped with a uint32_t map
  export_values<T>(*segment->dictionary(), file_path);
  export_compressed_vector(*segment->compressed_vector_type(), *segment->attribute_vector(), file_path);
  //TODO: What to do with non-compressed AttributeVectors?
}

void StorageManager::_write_segment_to_disk(const std::shared_ptr<AbstractSegment> abstract_segment,
                                            const std::string& file_path) const {
  resolve_data_type(abstract_segment->data_type(), [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    if constexpr (std::is_same<ColumnDataType, pmr_string>::value) {
      const auto fixed_string_dict_segment =
          dynamic_pointer_cast<FixedStringDictionarySegment<ColumnDataType>>(abstract_segment);
      _write_fixed_string_dict_segment_to_disk(fixed_string_dict_segment, file_path);
    } else {
      const auto dict_segment = dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(abstract_segment);
      _write_dict_segment_to_disk(dict_segment, file_path);
    }
  });
}

void StorageManager::_write_chunk_to_disk(const std::shared_ptr<Chunk> chunk,
                                          const std::vector<uint32_t>& segment_offset_ends,
                                          const std::string& file_path) const {
  auto header = CHUNK_HEADER{};
  header.row_count = chunk->size();
  header.segment_offset_ends = segment_offset_ends;

  export_value(header.row_count, file_path);

  for (const auto segment_offset_end : header.segment_offset_ends) {
    export_value(segment_offset_end, file_path);
  }

  const auto segment_count = chunk->column_count();
  for (auto segment_index = ColumnID{0}; segment_index < segment_count; ++segment_index) {
    const auto abstract_segment = chunk->get_segment(segment_index);
    _write_segment_to_disk(abstract_segment, file_path);
  }
}

std::pair<uint32_t, uint32_t> StorageManager::_persist_chunk_to_file(const std::shared_ptr<Chunk> chunk,
                                                                     ChunkID chunk_id,
                                                                     const std::string& file_name) const {
  const auto file_path = _resources_path + file_name;
  if (std::filesystem::exists(file_path)) {
    //append to existing file

    auto chunk_segment_offset_ends = _calculate_segment_offset_ends(chunk);
    auto chunk_offset_end = chunk_segment_offset_ends.back();

    // adapt and rewrite file header
    FILE_HEADER file_header = _read_file_header(file_path);
    const auto file_header_previous_chunk_count = file_header.chunk_count;
    const auto file_prev_chunk_end_offset = file_header.chunk_offset_ends[file_header_previous_chunk_count - 1];

    file_header.chunk_count = file_header.chunk_count + 1;
    file_header.chunk_ids[file_header_previous_chunk_count] = chunk_id;
    file_header.chunk_offset_ends[file_header_previous_chunk_count] = file_prev_chunk_end_offset + chunk_offset_end;

    overwrite_header(file_header, file_path);

    _write_chunk_to_disk(chunk, chunk_segment_offset_ends, file_path);

    const auto chunk_bytes = chunk_offset_end;
    const auto chunk_start_offset = file_prev_chunk_end_offset + _file_header_bytes;
    return std::make_pair(chunk_start_offset, chunk_bytes);
  }

  // create new file
  auto chunk_segment_offset_ends = _calculate_segment_offset_ends(chunk);
  auto chunk_offset_end = chunk_segment_offset_ends.back();

  auto fh = FILE_HEADER{};
  auto chunk_ids = std::array<uint32_t, MAX_CHUNK_COUNT_PER_FILE>();
  chunk_ids[0] = chunk_id;

  auto chunk_offset_ends = std::array<uint32_t, MAX_CHUNK_COUNT_PER_FILE>();
  chunk_offset_ends[0] = chunk_offset_end;

  fh.storage_format_version_id = _storage_format_version_id;
  fh.chunk_count = uint32_t{1};
  fh.chunk_ids = chunk_ids;
  fh.chunk_offset_ends = chunk_offset_ends;

  export_value<FILE_HEADER>(fh, file_path);

  _write_chunk_to_disk(chunk, chunk_segment_offset_ends, file_path);

  const auto chunk_bytes = chunk_offset_end;
  const auto chunk_start_offset = _file_header_bytes;
  return std::make_pair(chunk_start_offset, chunk_bytes);
}

void StorageManager::replace_chunk_with_persisted_chunk(const std::shared_ptr<Chunk> chunk, ChunkID chunk_id,
                                                        const Table* table_address) {
  const auto table_name = _get_table_name(table_address);
  Assert(!table_name.empty(), "Only tables registered with StorageManager can be persisted.");
  const auto table_persistence_file = _get_persistence_file_name(table_name);

  // persist chunk to disk
  auto [chunk_start_offset, chunk_bytes] = _persist_chunk_to_file(chunk, chunk_id, table_persistence_file);
  _tables_current_persistence_file_mapping[table_name].current_chunk_count++;

  // map chunk from disk
  const auto column_definitions = _tables[table_name]->column_data_types();
  auto mapped_chunk = _map_chunk_from_disk(chunk_start_offset, chunk_bytes, table_persistence_file,
                                           chunk->column_count(), column_definitions);
  //evaluate_mapped_chunk(chunk, mapped_chunk);

  mapped_chunk->set_mvcc_data(chunk->mvcc_data());

  // replace chunk in table
  _tables[table_name]->replace_chunk(chunk_id, mapped_chunk);
}

std::vector<std::shared_ptr<Chunk>> StorageManager::get_chunks_from_disk(
    std::string table_name, std::string file_name, const std::vector<TableColumnDefinition>& table_column_definitions) {
  const auto file_header = _read_file_header(file_name);
  auto chunks = std::vector<std::shared_ptr<Chunk>>{file_header.chunk_count};
  auto column_definitions = std::vector<DataType>{table_column_definitions.size()};

  for (auto index = size_t{0}; index < table_column_definitions.size(); ++index) {
    column_definitions[index] = table_column_definitions[index].data_type;
  }

  for (auto index = size_t{0}; index < file_header.chunk_count; ++index) {
    const auto chunk_bytes = file_header.chunk_offset_ends[index];
    auto chunk_start_offset = _file_header_bytes;

    if (index != 0)
      chunk_start_offset = file_header.chunk_offset_ends[index - 1] + _file_header_bytes;

    const auto chunk =
        _map_chunk_from_disk(chunk_start_offset, chunk_bytes, file_name, column_definitions.size(), column_definitions);
    chunks[index] = chunk;
  }

  return chunks;
}

const std::string StorageManager::_get_persistence_file_name(const std::string& table_name) {
  if (_tables_current_persistence_file_mapping[table_name].current_chunk_count == MAX_CHUNK_COUNT_PER_FILE) {
    const auto next_file_index = _tables_current_persistence_file_mapping[table_name].file_index + 1;
    auto next_persistence_file_name = table_name + "_" + std::to_string(next_file_index) + ".bin";
    _tables_current_persistence_file_mapping[table_name] = {next_persistence_file_name, next_file_index, 0};
  }
  return _tables_current_persistence_file_mapping[table_name].file_name;
}

FILE_HEADER StorageManager::_read_file_header(const std::string& filename) const {
  auto file_header = FILE_HEADER{};
  auto fd = int32_t{};

  Assert((fd = open((_resources_path + filename).c_str(), O_RDONLY)) >= 0, "Open error");
  auto* persisted_header =
      reinterpret_cast<uint32_t*>(mmap(NULL, _file_header_bytes, PROT_READ, MAP_PRIVATE, fd, off_t{0}));
  Assert((persisted_header != MAP_FAILED), "Mapping Failed");
  close(fd);

  file_header.storage_format_version_id = persisted_header[0];
  file_header.chunk_count = persisted_header[1];

  const auto header_constants_size = element_index(_format_version_id_bytes + _chunk_count_bytes, 4);

  for (auto header_index = size_t{0}; header_index < file_header.chunk_count; ++header_index) {
    file_header.chunk_ids[header_index] = persisted_header[header_constants_size + header_index];
    file_header.chunk_offset_ends[header_index] =
        persisted_header[header_constants_size + StorageManager::_chunk_count + header_index];
  }
  munmap(persisted_header, _file_header_bytes);

  return file_header;
}

CHUNK_HEADER StorageManager::_read_chunk_header(const std::byte* persisted_data, const uint32_t segment_count,
                                                const uint32_t chunk_offset_begin) const {
  auto header = CHUNK_HEADER{};
  const auto header_data = reinterpret_cast<const uint32_t*>(persisted_data);

  header.row_count = header_data[0];

  for (auto segment_offset_index = size_t{0}; segment_offset_index < segment_count; ++segment_offset_index) {
    header.segment_offset_ends.emplace_back(header_data[segment_offset_index + 1]);
  }

  return header;
}

std::shared_ptr<Chunk> StorageManager::_map_chunk_from_disk(const uint32_t chunk_offset_begin,
                                                            const uint32_t chunk_bytes, const std::string& filename,
                                                            const uint32_t segment_count,
                                                            const std::vector<DataType>& column_definitions) const {
  auto segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};
  auto fd = int32_t{};
  Assert((fd = open((_resources_path + filename).c_str(), O_RDONLY)) >= 0, "Opening of file failed.");

  //Calls to mmap need to be pagesize-aligned
  const auto pagesize = getpagesize();
  const auto difference_to_pagesize_alignment = chunk_offset_begin % pagesize;
  const auto page_size_aligned_offset = chunk_offset_begin - difference_to_pagesize_alignment;

  const auto* persisted_data =
      reinterpret_cast<std::byte*>(mmap(NULL, chunk_bytes + difference_to_pagesize_alignment, PROT_READ, MAP_PRIVATE,
                                        fd, off_t{page_size_aligned_offset}));
  Assert((persisted_data != MAP_FAILED), "Mapping of File Failed.");
  close(fd);

  persisted_data = persisted_data + difference_to_pagesize_alignment;

  const auto chunk_header = _read_chunk_header(persisted_data, segment_count, chunk_offset_begin);

  for (auto segment_index = size_t{0}; segment_index < segment_count; ++segment_index) {
    auto segment_offset_begin = _chunk_header_bytes(segment_count);

    if (segment_index > 0) {
      segment_offset_begin = chunk_header.segment_offset_ends[segment_index - 1];
    }

    resolve_data_type(column_definitions[segment_index], [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      if constexpr (std::is_same<ColumnDataType, pmr_string>::value) {
        segments.emplace_back(
            std::make_shared<FixedStringDictionarySegment<ColumnDataType>>(persisted_data + segment_offset_begin));
      } else {
        segments.emplace_back(
            std::make_shared<DictionarySegment<ColumnDataType>>(persisted_data + segment_offset_begin));
      }
    });
  }

  const auto chunk = std::make_shared<Chunk>(segments);
  return chunk;
}

uint32_t StorageManager::_chunk_header_bytes(uint32_t column_count) const {
  return _row_count_bytes + column_count * _segment_offset_bytes;
}

PersistedSegmentEncodingType StorageManager::_resolve_persisted_segment_encoding_type_from_compression_type(
    CompressedVectorType compressed_vector_type) const {
  PersistedSegmentEncodingType persisted_vector_type_id = {};
  switch (compressed_vector_type) {
    case CompressedVectorType::FixedWidthInteger4Byte:
      persisted_vector_type_id = PersistedSegmentEncodingType::DictionaryEncoding32Bit;
      break;
    case CompressedVectorType::FixedWidthInteger2Byte:
      persisted_vector_type_id = PersistedSegmentEncodingType::DictionaryEncoding16Bit;
      break;
    case CompressedVectorType::FixedWidthInteger1Byte:
      persisted_vector_type_id = PersistedSegmentEncodingType::DictionaryEncoding8Bit;
      break;
    case CompressedVectorType::BitPacking:
      persisted_vector_type_id = PersistedSegmentEncodingType::DictionaryEncodingBitPacking;
      break;
    default:
      persisted_vector_type_id = PersistedSegmentEncodingType::Unencoded;
  }
  return persisted_vector_type_id;
}

void StorageManager::_serialize_table_files_mapping() {
  for (const auto& mapping : _tables_current_persistence_file_mapping) {
    const auto table = get_table(mapping.first);
    const auto column_count = table->column_count();
    const auto chunk_count = mapping.second.file_index * MAX_CHUNK_COUNT_PER_FILE + mapping.second.current_chunk_count;
    json table_json = {{"file_name", mapping.second.file_name},
                       {"file_count", mapping.second.file_index + 1},
                       {"chunk_count", chunk_count},
                       {"column_count", static_cast<uint32_t>(table->column_count())}};

    const auto column_definitions = table->column_definitions();
    auto columns_json = json::array();
    for (auto index = size_t{0}; index < column_count; ++index) {
      const json column_object = {
          {"column_name", column_definitions[index].name},
          {"data_type", column_definitions[index].data_type},
          {"nullable", column_definitions[index].nullable},
      };
      columns_json.push_back(column_object);
    }
    table_json["columns"] = columns_json;

    _storage_json[mapping.first] = table_json;
  }
}

void StorageManager::save_storage_json_to_disk() {
  _serialize_table_files_mapping();
  std::ofstream output_file(_resources_path + _storage_json_name, std::ios::trunc);
  const auto json_serialized = std::string(_storage_json.dump(4));
  output_file << json_serialized;
  output_file.close();
}

std::vector<TableColumnDefinition> StorageManager::get_table_column_definitions_from_json(
    const std::string& table_name) {
  const auto table_json = _storage_json[table_name];

  auto table_column_definitions = std::vector<TableColumnDefinition>{table_json["columns"].size()};

  for (auto index = size_t{0}; index < table_json["columns"].size(); ++index) {
    const auto column = table_json["columns"][index];
    const auto table_column_definition =
        TableColumnDefinition(column["column_name"], column["data_type"], column["nullable"]);
    table_column_definitions[index] = table_column_definition;
  }

  return table_column_definitions;
}

void StorageManager::_load_storage_data_from_disk() {
  // Read the JSON data from disk into a string
  std::ifstream json_file(_resources_path + _storage_json_name);
  _storage_json = json::parse(json_file);

  // Deserialize the JSON into the map

  for (auto it = _storage_json.begin(); it != _storage_json.end(); ++it) {
    const auto& table_name = it.key();
    const auto item = it.value();
    const auto file_count = static_cast<uint32_t>(item["file_count"]);
    const auto file_index = file_count - 1;
    const auto file_name = table_name + "_" + std::to_string(file_index) + ".bin";
    const auto chunk_count = static_cast<uint32_t>(item["chunk_count"]) % MAX_CHUNK_COUNT_PER_FILE;

    Assert(chunk_count <= MAX_CHUNK_COUNT_PER_FILE, "Chunk count exceeds maximum chunk count per file.");
    PERSISTENCE_FILE_DATA data;
    data.file_name = file_name;
    data.file_index = file_index;
    data.current_chunk_count = chunk_count;

    _tables_current_persistence_file_mapping.emplace(table_name, std::move(data));
  }
}
}  // namespace hyrise
