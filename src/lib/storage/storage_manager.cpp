#include "storage_manager.hpp"

#include <sys/fcntl.h>
#include <sys/mman.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "import_export/file_type.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
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

uint32_t byte_index(uint32_t element_index, size_t element_size) {
  return element_index * element_size;
}

uint32_t element_index(uint32_t byte_index, size_t element_size) {
  return byte_index / element_size;
}

std::vector<uint32_t> StorageManager::generate_segment_offset_ends(const std::shared_ptr<Chunk> chunk) {
  const auto segment_count = chunk->column_count();
  auto segment_offset_ends = std::vector<uint32_t>(segment_count);

  auto offset_end = CHUNK_HEADER_BYTES;
  for (auto segment_index = size_t{0}; segment_index < segment_count; ++segment_index) {
    offset_end += SEGMENT_HEADER_BYTES;

    const auto abstract_segment = chunk->get_segment(static_cast<ColumnID>(static_cast<uint16_t>(segment_index)));
    const auto dict_segment = dynamic_pointer_cast<DictionarySegment<int>>(abstract_segment);

    offset_end += byte_index(dict_segment->dictionary()->size(), 4);

    const auto attribute_vector = dict_segment->attribute_vector();
    const auto attribute_vector_type = attribute_vector->type();

    switch (attribute_vector_type) {
      case CompressedVectorType::FixedWidthInteger4Byte:
        offset_end += byte_index(attribute_vector->size(), 4);
        break;
      case CompressedVectorType::FixedWidthInteger2Byte:
        offset_end += byte_index(attribute_vector->size(), 2);
        break;
      case CompressedVectorType::FixedWidthInteger1Byte:
        offset_end += attribute_vector->size();
        break;
      case CompressedVectorType::BitPacking:
        offset_end += 4;
        offset_end += dynamic_cast<const BitPackingVector&>(*attribute_vector).data().bytes();
        break;
      default:
        Fail("Any other type should have been caught before.");
    }

    segment_offset_ends[segment_index] = offset_end;
  }
  return segment_offset_ends;
}

void StorageManager::prepare_filestream() {
  FILESTREAM.open(FILENAME, std::ios::out | std::ios::binary | std::ios::app);
}

void StorageManager::end_filestream() {
  FILESTREAM.close();
}

/*
 * Copied binary writing functions from `binary_writer.cpp`
 */

template <typename T>
void export_value(std::ofstream& ofstream, const T& value) {
  ofstream.write(reinterpret_cast<const char*>(&value), sizeof(T));
}

template <typename T, typename Alloc>
void export_values(std::ofstream& ofstream, const std::vector<T, Alloc>& values) {
  ofstream.write(reinterpret_cast<const char*>(values.data()), values.size() * sizeof(T));
}

template <typename T>
CompressedVectorTypeID infer_compressed_vector_type_id(
    const AbstractEncodedSegment& abstract_encoded_segment) {
  uint8_t compressed_vector_type_id = 0u;
  resolve_encoded_segment_type<T>(abstract_encoded_segment, [&compressed_vector_type_id](auto& typed_segment) {
    const auto compressed_vector_type = typed_segment.compressed_vector_type();
    Assert(compressed_vector_type, "Expected Segment to use vector compression");
    switch (*compressed_vector_type) {
      case CompressedVectorType::FixedWidthInteger4Byte:
      case CompressedVectorType::FixedWidthInteger2Byte:
      case CompressedVectorType::FixedWidthInteger1Byte:
      case CompressedVectorType::BitPacking:
        compressed_vector_type_id = static_cast<uint8_t>(*compressed_vector_type);
        break;
      default:
        Fail("Export of specified CompressedVectorType is not yet supported");
    }
  });
  return compressed_vector_type_id;
}

// needed for attribute vector which is stored in a compact manner
void export_compact_vector(std::ofstream& ofstream, const pmr_compact_vector& values) {
  //adapted to uint32_t format of later created map (see comment in `write_dict_segment_to_disk`)
  export_value(ofstream, static_cast<uint32_t>(values.bits()));
  ofstream.write(reinterpret_cast<const char*>(values.get()), static_cast<int64_t>(values.bytes()));
}

void export_compressed_vector(std::ofstream& ofstream, const CompressedVectorType type,
                              const BaseCompressedVector& compressed_vector) {
  switch (type) {
    case CompressedVectorType::FixedWidthInteger4Byte:
      export_values(ofstream, dynamic_cast<const FixedWidthIntegerVector<uint32_t>&>(compressed_vector).data());
      return;
    case CompressedVectorType::FixedWidthInteger2Byte:
      export_values(ofstream, dynamic_cast<const FixedWidthIntegerVector<uint16_t>&>(compressed_vector).data());
      return;
    case CompressedVectorType::FixedWidthInteger1Byte:
      export_values(ofstream, dynamic_cast<const FixedWidthIntegerVector<uint8_t>&>(compressed_vector).data());
      return;
    case CompressedVectorType::BitPacking:
      export_compact_vector(ofstream, dynamic_cast<const BitPackingVector&>(compressed_vector).data());
      return;
    default:
      Fail("Any other type should have been caught before.");
  }
}

void StorageManager::write_dict_segment_to_disk(const std::shared_ptr<DictionarySegment<int>> segment) {
  /*
   * Write dict segment to given file using the following format:
   * 1. Number of elements in dictionary
   * 2. Number of elements in attribute_vector
   * 3. AttributeVectorCompressionID aka. size of int used in attribute vector
   * 3. Dictionary values
   * 4. Attribute_vector values
   *
   * For this exercise we assume an <int>-DictionarySegment with an FixedWidthIntegerVector<uint16_t> attribute_vector.
   * As a next step we should use the AttributeVectorCompressionID to define the type of the FixedWidthIntegerVector
   * and perhaps also write out the type of the DictionarySegment.
   */

  //TODO: Should this be continued?
  // We will later mmap to an uint32_t vector/array. Therefore, we store all metadata points as uint32_t.
  // This wastes up to three bytes of compression per metadata point but makes mapping much easier.
  export_value(FILESTREAM, static_cast<uint32_t>(segment->dictionary()->size()));
  export_value(FILESTREAM, static_cast<uint32_t>(segment->attribute_vector()->size()));

  const auto compressed_vector_type_id = static_cast<uint32_t>(infer_compressed_vector_type_id<int>(*segment));
  export_value(FILESTREAM, compressed_vector_type_id);

  export_values<int32_t>(FILESTREAM, *segment->dictionary());
  export_compressed_vector(FILESTREAM, *segment->compressed_vector_type(),
                           *segment->attribute_vector());
}

void StorageManager::write_chunk_to_disk(const std::shared_ptr<Chunk> chunk, const std::vector<uint32_t> segment_offset_ends) {
  chunk_header header;
  header.row_count = chunk->size();
  header.segment_offset_ends = segment_offset_ends;

  std::cout << "RowCount: " << header.row_count << std::endl;
  for (const auto segment_offset_end : header.segment_offset_ends) {
    std::cout << "+" << segment_offset_end;
  }
  std::cout << std::endl;

  export_value(FILESTREAM, header.row_count);

  for (const auto segment_offset_end : header.segment_offset_ends) {
    export_value(FILESTREAM, segment_offset_end);
  }

  const auto segment_count = chunk->column_count();
  for (auto segment_index = size_t{0}; segment_index < segment_count; ++segment_index) {
    const auto abstract_segment = chunk->get_segment(static_cast<ColumnID>(static_cast<uint16_t>(segment_index)));
    const auto dict_segment = dynamic_pointer_cast<DictionarySegment<int>>(abstract_segment);

    // debug print to verify written content
    if (segment_index == 0) {
      std::cout << "Dict: ";
      for (auto i = size_t{0}; i < 20; ++i) {
        std::cout << dict_segment->dictionary()->at(i) << " ";
      }
      std::cout << "\nAtt: ";
      for (auto i = size_t{0}; i < 20; ++i) {
        std::cout << dynamic_pointer_cast<const FixedWidthIntegerVector<uint16_t>>(dict_segment->attribute_vector())->data().at(i) << " ";
      }
      std::cout  << std::endl;
    }

    write_dict_segment_to_disk(dict_segment);
  }
}

void StorageManager::persist_chunks_to_disk(std::vector<std::shared_ptr<Chunk>> chunks) {
  // file_lock.acquire();

  auto chunk_segment_offset_ends = std::vector<std::vector<uint32_t>>(StorageManager::CHUNK_COUNT);
  auto chunk_offset_ends = std::array<uint32_t, StorageManager::CHUNK_COUNT>();
  auto chunk_ids = std::array<uint32_t, StorageManager::CHUNK_COUNT>();

  auto offset = uint32_t{sizeof(file_header)};
  for (auto chunk_index = uint32_t{0}; chunk_index < chunks.size(); ++chunk_index) {
    const auto segment_offset_ends = generate_segment_offset_ends(chunks[chunk_index]);
    offset += segment_offset_ends.back();

    chunk_segment_offset_ends[chunk_index] = segment_offset_ends;
    chunk_offset_ends[chunk_index] = offset;
  }
  // Fill all offset fields, that are not used with 0s.
  for (auto rest_chunk_index = chunks.size(); rest_chunk_index < StorageManager::CHUNK_COUNT; ++rest_chunk_index) {
    chunk_offset_ends[rest_chunk_index] = uint32_t{0};
  }

  // TODO(everyone): Find, how to get the actual chunk id.
  for (auto index = uint32_t{0}; index < StorageManager::CHUNK_COUNT; ++index) {
    chunk_ids[index] = index;
  }

  file_header fh;
  fh.storage_format_version_id = 2;
  fh.chunk_count = static_cast<uint16_t>(chunks.size());
  fh.chunk_ids = chunk_ids;
  fh.chunk_offset_ends = chunk_offset_ends;

  export_value<file_header>(StorageManager::FILESTREAM, fh);

  for (auto chunk_index = uint32_t{0}; chunk_index < chunks.size(); ++chunk_index) {
    const auto chunk = chunks[chunk_index];
    write_chunk_to_disk(chunk, chunk_segment_offset_ends[chunk_index]);
  }

  // file_lock.release();
}

file_header StorageManager::read_file_header(std::string filename) {
  file_header file_header;

  auto fd = int32_t{};
  Assert((fd = open(filename.c_str(), O_RDONLY) >= 0), "Open error");
  auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, sizeof(file_header), PROT_READ, MAP_PRIVATE, fd, off_t{0}));
  Assert((map != MAP_FAILED), "Mapping Failed");
  close(fd);

  file_header.storage_format_version_id = map[0];
  file_header.chunk_count = map[1];

  const auto fixed_mapped_element_count = 2;

  for (auto header_index = size_t{0}; header_index < file_header.chunk_count; ++header_index) {
    file_header.chunk_ids[header_index] = map[fixed_mapped_element_count + header_index];
    file_header.chunk_offset_ends[header_index] = map[fixed_mapped_element_count + StorageManager::CHUNK_COUNT + header_index];
  }

  return file_header;
}





}  // namespace hyrise
