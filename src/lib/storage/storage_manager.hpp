#pragma once

#include <tbb/concurrent_unordered_map.h>

#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "chunk.hpp"
#include "lqp_view.hpp"

#ifdef __APPLE__
#include "nlohmann/json.hpp"
#endif

#ifdef __linux__
#include "../../third_party/nlohmann_json/single_include/nlohmann/json.hpp"
#endif

#include "prepared_plan.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
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

struct PERSISTENCE_FILE_DATA {
  std::string file_name;
  uint32_t file_index;
  uint32_t current_chunk_count;
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

  /** @} */

  // For debugging purposes mostly, dump all tables as csv
  void export_all_tables_as_csv(const std::string& path);

  void persist_chunks_to_disk(const std::vector<std::shared_ptr<Chunk>>& chunks, const std::string& file_name);
  std::pair<uint32_t, uint32_t> persist_chunk_to_file(const std::shared_ptr<Chunk> chunk, ChunkID chunk_id,
                                                      const std::string& file_name);

  void replace_chunk_with_persisted_chunk(const std::shared_ptr<Chunk> chunk, ChunkID chunk_id,
                                          const Table* table_address);

  std::vector<std::shared_ptr<Chunk>> get_chunks_from_disk(
      std::string table_name, std::string file_name,
      const std::vector<TableColumnDefinition>& table_column_definitions);

  std::vector<TableColumnDefinition> get_table_column_definitions_from_json(const std::string& table_name);

  uint32_t get_max_chunk_count_per_file() {
    return _chunk_count;
  }

  uint32_t get_storage_format_version_id() {
    return _storage_format_version_id;
  }

  tbb::concurrent_unordered_map<std::string, PERSISTENCE_FILE_DATA> get_tables_files_mapping() {
    return _tables_current_persistence_file_mapping;
  }

  void save_storage_json_to_disk();

  uint32_t get_file_header_bytes() {
    return _file_header_bytes;
  }

 protected:
  friend class Hyrise;

  StorageManager() {
    std::ifstream const json_file(_resources_path + _storage_json_name);
    // If the file exists, load the contents into the json object.
    if (json_file.good()) {
      _load_storage_data_from_disk();
    }
  }

  std::string _resources_path = "resources/";
  std::string _storage_json_name = "storage.json";
  nlohmann::json _storage_json;

  // We preallocate maps to prevent costly re-allocation.
  static constexpr size_t INITIAL_MAP_SIZE = 100;

  tbb::concurrent_unordered_map<std::string, std::shared_ptr<Table>> _tables{INITIAL_MAP_SIZE};
  tbb::concurrent_unordered_map<std::string, PERSISTENCE_FILE_DATA> _tables_current_persistence_file_mapping{
      INITIAL_MAP_SIZE};
  tbb::concurrent_unordered_map<std::string, std::shared_ptr<LQPView>> _views{INITIAL_MAP_SIZE};
  tbb::concurrent_unordered_map<std::string, std::shared_ptr<PreparedPlan>> _prepared_plans{INITIAL_MAP_SIZE};

 private:
  static constexpr uint32_t _chunk_count = MAX_CHUNK_COUNT_PER_FILE;
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

 private:
  CHUNK_HEADER _read_chunk_header(const std::byte* map, const uint32_t segment_count,
                                  const uint32_t chunk_offset_begin) const;

  FILE_HEADER _read_file_header(const std::string& filename) const;

  std::vector<uint32_t> _calculate_segment_offset_ends(const std::shared_ptr<Chunk> chunk) const;

  std::pair<uint32_t, uint32_t> _persist_chunk_to_file(const std::shared_ptr<Chunk> chunk, ChunkID chunk_id,
                                                       const std::string& file_name) const;

  template <typename T>
  void _write_dict_segment_to_disk(const std::shared_ptr<DictionarySegment<T>> segment,
                                   const std::string& file_name) const;

  template <typename T>
  void _write_fixed_string_dict_segment_to_disk(const std::shared_ptr<FixedStringDictionarySegment<T>> segment,
                                                const std::string& file_name) const;

  void _write_chunk_to_disk(const std::shared_ptr<Chunk> chunk, const std::vector<uint32_t>& segment_offset_ends,
                            const std::string& file_name) const;

  void _write_segment_to_disk(const std::shared_ptr<AbstractSegment> abstract_segment,
                              const std::string& file_name) const;

  uint32_t _chunk_header_bytes(const uint32_t column_count) const;

  const std::string _get_persistence_file_name(const std::string& table_name);

  std::shared_ptr<Chunk> _map_chunk_from_disk(const uint32_t chunk_offset_end, const uint32_t chunk_bytes,
                                              const std::string& filename, const uint32_t segment_count,
                                              const std::vector<DataType>& column_definitions) const;

  PersistedSegmentEncodingType _resolve_persisted_segment_encoding_type_from_compression_type(
      const CompressedVectorType compressed_vector_type) const;

  std::string _get_table_name(const Table* address) const;

  void _load_storage_data_from_disk();
  void _serialize_table_files_mapping();
};

std::ostream& operator<<(std::ostream& stream, const StorageManager& storage_manager);

}  // namespace hyrise
