#pragma once

#include <tbb/concurrent_unordered_map.h>

#include <iostream>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>
#include <semaphore>
#include <fstream>

#include "storage/chunk_encoder.hpp"
#include "lqp_view.hpp"
#include "prepared_plan.hpp"
#include "types.hpp"

#include "storage/dictionary_segment.hpp"

namespace hyrise {

class Table;
class AbstractLQPNode;

struct file_header {
  uint32_t storage_format_version_id;
  uint32_t chunk_count;
  std::array<uint32_t, 50> chunk_ids;
  std::array<uint32_t, 50> chunk_offset_ends;
};

struct chunk_header {
  uint32_t row_count;
  std::vector<uint32_t> segment_offset_ends;
};

// The StorageManager is a class that maintains all tables
// by mapping table names to table instances.
class StorageManager : public Noncopyable {
 public:
  /**
   * @defgroup Manage Tables, this is only thread-safe for operations on tables with different names
   * @{
   */
  void add_table(const std::string& name, std::shared_ptr<Table> table);
  void drop_table(const std::string& name);
  std::shared_ptr<Table> get_table(const std::string& name) const;
  bool has_table(const std::string& name) const;
  std::vector<std::string> table_names() const;
  std::unordered_map<std::string, std::shared_ptr<Table>> tables() const;
  /** @} */

  /**
   * @defgroup Manage SQL VIEWs, this is only thread-safe for operations on views with different names
   * @{
   */
  void add_view(const std::string& name, const std::shared_ptr<LQPView>& view);
  void drop_view(const std::string& name);
  std::shared_ptr<LQPView> get_view(const std::string& name) const;
  bool has_view(const std::string& name) const;
  std::vector<std::string> view_names() const;
  std::unordered_map<std::string, std::shared_ptr<LQPView>> views() const;
  /** @} */

  /**
   * @defgroup Manage prepared plans - comparable to SQL PREPAREd statements, this is only thread-safe for operations on prepared plans with different names
   * @{
   */
  void add_prepared_plan(const std::string& name, const std::shared_ptr<PreparedPlan>& prepared_plan);
  std::shared_ptr<PreparedPlan> get_prepared_plan(const std::string& name) const;
  bool has_prepared_plan(const std::string& name) const;
  void drop_prepared_plan(const std::string& name);
  std::unordered_map<std::string, std::shared_ptr<PreparedPlan>> prepared_plans() const;
  /** @} */

  // For debugging purposes mostly, dump all tables as csv
  void export_all_tables_as_csv(const std::string& path);

 protected:
  StorageManager() = default;
  friend class Hyrise;

  // We preallocate maps to prevent costly re-allocation.
  static constexpr size_t INITIAL_MAP_SIZE = 100;

  tbb::concurrent_unordered_map<std::string, std::shared_ptr<Table>> _tables{INITIAL_MAP_SIZE};
  tbb::concurrent_unordered_map<std::string, std::shared_ptr<LQPView>> _views{INITIAL_MAP_SIZE};
  tbb::concurrent_unordered_map<std::string, std::shared_ptr<PreparedPlan>> _prepared_plans{INITIAL_MAP_SIZE};

  void persist_chunks_to_disk(std::vector<std::shared_ptr<Chunk>> chunks, std::string file_name);
  file_header read_file_header(std::string filename);

 private:
  static const uint32_t CHUNK_COUNT = 50;

  // Fileformat constants
  // File Header
  uint32_t FORMAT_VERSION_ID_BYTES = 2;
  uint32_t CHUNK_COUNT_BYTES = 2;
  uint32_t CHUNK_ID_BYTES = 4;
  uint32_t CHUNK_OFFSET_BYTES = 4;
  uint32_t FILE_HEADER_BYTES = FORMAT_VERSION_ID_BYTES + CHUNK_COUNT_BYTES + CHUNK_COUNT * CHUNK_ID_BYTES + CHUNK_COUNT * CHUNK_OFFSET_BYTES;

  // Chunk Header
  uint32_t ROW_COUNT_BYTES = 4;
  uint32_t SEGMENT_OFFSET_BYTES = 4;
  uint32_t CHUNK_HEADER_BYTES(uint32_t COLUMN_COUNT) {
    return ROW_COUNT_BYTES + COLUMN_COUNT * SEGMENT_OFFSET_BYTES;
  }

  // Segment Header
  uint32_t DICTIONARY_SIZE_BYTES = 4;
  uint32_t ELEMENT_COUNT_BYTES = 4;
  uint32_t COMPRESSED_VECTOR_TYPE_ID_BYTES = 4;
  uint32_t SEGMENT_HEADER_BYTES = DICTIONARY_SIZE_BYTES + ELEMENT_COUNT_BYTES + COMPRESSED_VECTOR_TYPE_ID_BYTES;

  std::vector<uint32_t> generate_segment_offset_ends(const std::shared_ptr<Chunk> chunk);
  void write_dict_segment_to_disk(const std::shared_ptr<DictionarySegment<int>> segment);
  void write_chunk_to_disk(const std::shared_ptr<Chunk> chunk, const std::vector<uint32_t> segment_offset_ends);
};

std::ostream& operator<<(std::ostream& stream, const StorageManager& storage_manager);

}  // namespace hyrise
