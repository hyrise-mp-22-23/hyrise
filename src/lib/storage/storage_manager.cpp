#include "storage_manager.hpp"

#include <sys/fcntl.h>
#include <sys/mman.h>
#include <memory>
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

  auto offset_end = CHUNK_HEADER_BYTES(segment_count);
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

/*
 * Copied binary writing functions from `binary_writer.cpp`
 */

template <typename T>
void export_value(const T& value, std::string file_name) {
  std::ofstream ofstream(file_name, std::ios::binary | std::ios::app);
  ofstream.write(reinterpret_cast<const char*>(&value), sizeof(T));
  ofstream.close();
}

template <typename T, typename Alloc>
void export_values(const std::vector<T, Alloc>& values, std::string file_name) {
  std::ofstream ofstream(file_name, std::ios::binary | std::ios::app);
  ofstream.write(reinterpret_cast<const char*>(values.data()), values.size() * sizeof(T));
  ofstream.close();
}

// needed for attribute vector which is stored in a compact manner
void export_compact_vector(const pmr_compact_vector& values, std::string file_name) {
  //adapted to uint32_t format of later created map (see comment in `write_dict_segment_to_disk`)
  export_value(static_cast<uint32_t>(values.bits()), file_name);
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

void StorageManager::write_dict_segment_to_disk(const std::shared_ptr<DictionarySegment<int>> segment,
                                                const std::string& file_name) {
  /*
   * For a description of how dictionary segments look, see the following PR:
   *    https://github.com/hyrise-mp-22-23/hyrise/pull/94
   */
  export_value(static_cast<uint32_t>(segment->dictionary()->size()), file_name);
  export_value(static_cast<uint32_t>(segment->attribute_vector()->size()), file_name);

  const auto compressed_vector_type_id = static_cast<uint32_t>(BinaryWriter::_compressed_vector_type_id<int>(*segment));
  export_value(compressed_vector_type_id, file_name);

  export_values<int32_t>(*segment->dictionary(), file_name);
  export_compressed_vector(*segment->compressed_vector_type(), *segment->attribute_vector(), file_name);
}

void StorageManager::write_chunk_to_disk(const std::shared_ptr<Chunk>& chunk,
                                         const std::vector<uint32_t>& segment_offset_ends, const std::string& file_name) {
  chunk_header header;
  header.row_count = chunk->size();
  header.segment_offset_ends = segment_offset_ends;

  export_value(header.row_count, file_name);

  for (const auto segment_offset_end : header.segment_offset_ends) {
    export_value(segment_offset_end, file_name);
  }

  const auto segment_count = chunk->column_count();
  for (auto segment_index = size_t{0}; segment_index < segment_count; ++segment_index) {
    const auto abstract_segment = chunk->get_segment(static_cast<ColumnID>(static_cast<uint16_t>(segment_index)));
    const auto dict_segment = dynamic_pointer_cast<DictionarySegment<int>>(abstract_segment);

    write_dict_segment_to_disk(dict_segment, file_name);
  }
}

void StorageManager::persist_chunks_to_disk(const std::vector<std::shared_ptr<Chunk>>& chunks, const std::string& file_name) {
  /*
    TODO(everyone): Think about a proper implementation of a locking method. Each written file needs to be
    locked prior to writing (and released afterwards). It was decided to use the mutex class by cpp.
    (see https://en.cppreference.com/w/cpp/thread/mutex).
  */
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
  for (auto chunk_index = chunks.size(); chunk_index < StorageManager::CHUNK_COUNT; ++chunk_index) {
    chunk_offset_ends[chunk_index] = uint32_t{0};
  }

  // TODO(everyone): Find, how to get the actual chunk id.
  for (auto index = uint32_t{0}; index < StorageManager::CHUNK_COUNT; ++index) {
    chunk_ids[index] = index;
  }

  file_header fh;
  fh.storage_format_version_id = STORAGE_FORMAT_VERSION_ID;
  fh.chunk_count = static_cast<uint32_t>(chunks.size());
  fh.chunk_ids = chunk_ids;
  fh.chunk_offset_ends = chunk_offset_ends;

  export_value<file_header>(fh, file_name);

  for (auto chunk_index = uint32_t{0}; chunk_index < chunks.size(); ++chunk_index) {
    const auto chunk = chunks[chunk_index];
    write_chunk_to_disk(chunk, chunk_segment_offset_ends[chunk_index], file_name);
  }

  // file_lock.release();
}

file_header StorageManager::read_file_header(const std::string& filename) {
  file_header file_header;
  auto fd = int32_t{};

  Assert((fd = open(filename.c_str(), O_RDONLY)) >= 0, "Open error");
  auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, FILE_HEADER_BYTES, PROT_READ, MAP_PRIVATE, fd, off_t{0}));
  Assert((map != MAP_FAILED), "Mapping Failed");
  close(fd);

  file_header.storage_format_version_id = map[0];
  file_header.chunk_count = map[1];

  const auto header_constants_size = element_index(FORMAT_VERSION_ID_BYTES + CHUNK_COUNT_BYTES, 4);

  for (auto header_index = size_t{0}; header_index < file_header.chunk_count; ++header_index) {
    file_header.chunk_ids[header_index] = map[header_constants_size + header_index];
    file_header.chunk_offset_ends[header_index] =
        map[header_constants_size + StorageManager::CHUNK_COUNT + header_index];
  }
  munmap(map, FILE_HEADER_BYTES);

  return file_header;
}

chunk_header StorageManager::read_chunk_header(const std::string& filename, const uint32_t segment_count,
                                               const uint32_t chunk_offset_begin) {
  // TODO: Remove need to map the whole file.
  chunk_header header;
  const auto map_index = element_index(chunk_offset_begin, 4);

  auto fd = int32_t{};
  Assert((fd = open(filename.c_str(), O_RDONLY)) >= 0, "Opening of file failed.");

  const auto file_bytes = std::filesystem::file_size(filename);
  auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, file_bytes, PROT_READ, MAP_PRIVATE, fd, off_t{0}));
  Assert((map != MAP_FAILED), "Mapping of Chunk Failed.");
  close(fd);

  header.row_count = map[map_index];

  for (auto segment_offset_index = size_t{0}; segment_offset_index < segment_count + 1; ++segment_offset_index) {
    header.segment_offset_ends.emplace_back(map[segment_offset_index + map_index + 1]);
  }

  return header;
}

std::shared_ptr<Chunk> StorageManager::map_chunk_from_disk(const uint32_t chunk_offset_end, const std::string& filename,
                                                           const uint32_t segment_count) {
  auto segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};

  auto fd = int32_t{};
  Assert((fd = open(filename.c_str(), O_RDONLY)) >= 0, "Opening of file failed.");

  const auto file_bytes = std::filesystem::file_size(filename);

  // TODO: Remove unneccesary map on whole file
  const auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, file_bytes, PROT_READ, MAP_PRIVATE, fd, off_t{0}));
  Assert((map != MAP_FAILED), "Mapping of File Failed.");
  close(fd);

  const auto header = read_chunk_header(filename, segment_count, chunk_offset_end);
  const auto header_offset = element_index(chunk_offset_end, 4);

  for (auto segment_index = size_t{0}; segment_index < segment_count; ++segment_index) {
    auto segment_offset_end = CHUNK_HEADER_BYTES(segment_count);
    if (segment_index > 0) {
      segment_offset_end = header.segment_offset_ends[segment_index - 1];
    }

    const auto segment_element_offset_index = element_index(segment_offset_end, 4);
    const auto dictionary_size = map[header_offset + segment_element_offset_index];
    const auto attribute_vector_size = map[header_offset + segment_element_offset_index + 1];

    auto dictionary_values = pmr_vector<int32_t>(dictionary_size);
    memcpy(dictionary_values.data(), &map[header_offset + segment_element_offset_index + 3],
           dictionary_size * sizeof(uint32_t));
    auto dictionary = std::make_shared<pmr_vector<int32_t>>(dictionary_values);

    const auto encoding_type = map[header_offset + segment_offset_end / 4 + 2];

    switch (encoding_type) {
      case DICT_ENCODING_8_BIT: {
        auto attribute_values = pmr_vector<uint8_t>(attribute_vector_size);
        memcpy(attribute_values.data(), &map[header_offset + segment_element_offset_index + 3 + dictionary_size],
               attribute_vector_size * sizeof(uint8_t));
        auto attribute_vector = std::make_shared<FixedWidthIntegerVector<uint8_t>>(attribute_values);

        const auto dictionary_segment = std::make_shared<DictionarySegment<int>>(dictionary, attribute_vector);
        segments.emplace_back(dynamic_pointer_cast<AbstractSegment>(dictionary_segment));
        break;
      }
      case DICT_ENCODING_16_BIT: {
        auto attribute_values = pmr_vector<uint16_t>(attribute_vector_size);
        memcpy(attribute_values.data(), &map[header_offset + segment_element_offset_index + 3 + dictionary_size],
               attribute_vector_size * sizeof(uint16_t));
        auto attribute_vector = std::make_shared<FixedWidthIntegerVector<uint16_t>>(attribute_values);

        const auto dictionary_segment = std::make_shared<DictionarySegment<int>>(dictionary, attribute_vector);
        segments.emplace_back(dynamic_pointer_cast<AbstractSegment>(dictionary_segment));
        break;
      }
      default:
        Fail("Unknown Compression Type");
    }
  }

  const auto chunk = std::make_shared<Chunk>(segments);
  return chunk;
}

}  // namespace hyrise
