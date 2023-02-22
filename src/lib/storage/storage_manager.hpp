#pragma once

#include <tbb/concurrent_unordered_map.h>

#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <nlohmann/json.hpp>
#include <shared_mutex>
#include <string>
#include <vector>

#include "chunk.hpp"
#include "lqp_view.hpp"
#include "prepared_plan.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/dictionary_segment.hpp"
#include "types.hpp"

namespace hyrise {

class Table;
class AbstractLQPNode;

const auto MAX_CHUNK_COUNT_PER_FILE = uint8_t{50};

struct FILE_HEADER {
  uint32_t storage_format_version_id;
  uint32_t chunk_count;
  std::array<uint32_t, MAX_CHUNK_COUNT_PER_FILE> chunk_ids;
  std::array<uint32_t, MAX_CHUNK_COUNT_PER_FILE> chunk_offset_ends;
};

struct CHUNK_HEADER {
  uint32_t row_count;
  std::vector<uint32_t> segment_offset_ends;
};

enum class PersistedSegmentEncodingType : uint32_t {
  Unencoded,
  DictionaryEncoding8Bit,
  DictionaryEncoding16Bit,
  DictionaryEncoding32Bit,
  DictionaryEncodingBitPacking
};

// The StorageManager is a class that maintains all tables
// by mapping table names to table instances.
class StorageManager : public Noncopyable {
  friend class Hyrise;
  friend class StorageManagerTest;

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

  /**
     * @defgroup Manage writing Chunks to disk and keep storage.json synchronized.
     * @{
     */
  void write_to_disk(const Chunk* chunk);
  void update_json(const std::string& table_name);
  void update_json_chunk(const Chunk* chunk);
  ssize_t find_table_index_in_json(const std::string& table_name) const;
  ssize_t find_chunk_index_in_json(const nlohmann::json& table_json, const size_t chunk_id) const;

  /** @} */

  // For debugging purposes mostly, dump all tables as csv
  void export_all_tables_as_csv(const std::string& path);

  void persist_chunks_to_disk(const std::vector<std::shared_ptr<Chunk>>& chunks, const std::string& file_name);

  FILE_HEADER read_file_header(const std::string& filename);
  std::shared_ptr<Chunk> map_chunk_from_disk(const uint32_t chunk_offset_end, const std::string& filename,
                                             const uint32_t segment_count);

  uint32_t get_max_chunk_count_per_file() {
    return _chunk_count;
  }

  uint32_t get_storage_format_version_id() {
    return _storage_format_version_id;
  }

  void flush_storage_json();

 protected:
  friend class Hyrise;

  //StorageManager() = default;
  StorageManager() {
    std::ifstream json_file(_storage_json_path);
    // If the file exists, load the contents into the json object.
    if (json_file.good()) {
      json_file >> _storage_json;
    }
  }

  // We preallocate maps to prevent costly re-allocation.
  static constexpr size_t INITIAL_MAP_SIZE = 100;
  const char* _storage_json_path = "./resources/storage.json";

  tbb::concurrent_unordered_map<std::string, std::shared_ptr<Table>> _tables{INITIAL_MAP_SIZE};
  tbb::concurrent_unordered_map<std::string, std::shared_ptr<LQPView>> _views{INITIAL_MAP_SIZE};
  tbb::concurrent_unordered_map<std::string, std::shared_ptr<PreparedPlan>> _prepared_plans{INITIAL_MAP_SIZE};
  nlohmann::json _storage_json;

 private:
  static constexpr uint32_t _chunk_count = 50;
  static constexpr uint32_t _storage_format_version_id = 1;

  // Fileformat constants
  // File Header
  static constexpr uint32_t _format_version_id_bytes = 4;
  static constexpr uint32_t _chunk_count_bytes = 4;
  static constexpr uint32_t _chunk_id_bytes = 4;
  static constexpr uint32_t _chunk_offset_bytes = 4;
  static constexpr uint32_t _file_header_bytes = _format_version_id_bytes + _chunk_count_bytes +
                                                 _chunk_count * _chunk_id_bytes + _chunk_count * _chunk_offset_bytes;

  // Chunk Header
  static constexpr uint32_t _row_count_bytes = 4;
  static constexpr uint32_t _segment_offset_bytes = 4;

  // Segment Header
  static constexpr uint32_t _dictionary_size_bytes = 4;
  static constexpr uint32_t _element_count_bytes = 4;
  static constexpr uint32_t _compressed_vector_type_id_bytes = 4;
  static constexpr uint32_t _segment_header_bytes =
      _dictionary_size_bytes + _element_count_bytes + _compressed_vector_type_id_bytes;

  CHUNK_HEADER read_chunk_header(const std::string& filename, const uint32_t segment_count,
                                 const uint32_t chunk_offset_begin);

  std::vector<uint32_t> generate_segment_offset_ends(const std::shared_ptr<Chunk> chunk);
  void write_dict_segment_to_disk(const std::shared_ptr<DictionarySegment<int>> segment, const std::string& file_name);
  void write_chunk_to_disk(const std::shared_ptr<Chunk>& chunk, const std::vector<uint32_t>& segment_offset_ends,
                           const std::string& file_name);
  uint32_t _chunk_header_bytes(uint32_t column_count);
};

std::ostream& operator<<(std::ostream& stream, const StorageManager& storage_manager);

}  // namespace hyrise
