#include <memory>

#include "hyrise.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/table.hpp"

namespace hyrise {

class StorageManagerTestUtil {
 public:
  static std::shared_ptr<Chunk> create_dictionary_segment_chunk(const uint32_t row_count, const uint32_t column_count) {
    /*
   * Create a chunk with index-times repeating elements in each segment.
   * Example: in segment 0 every value is unique, in segment 1 every value appears twice, in segment 2 thrice ...
   * Dictionary-encode each segment and return dictionary encoded chunk.
   */
    auto segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};
    for (auto segment_index = uint32_t{0}; segment_index < column_count; ++segment_index) {
      auto new_value_segment = std::make_shared<ValueSegment<int32_t>>(false, ChunkOffset{row_count});

      auto current_value = static_cast<int32_t>(row_count);
      auto value_count = uint32_t{1};  //start 1-indexed to avoid issues with modulo operations
      while (value_count - 1 <
             row_count) {  //as we start 1-indexed we need to adapt while-condition to create row-count many elements
        new_value_segment->append(current_value);

        //create segment-index many duplicates of each value in the segment
        if (value_count % (segment_index + 1) == 0) {
          --current_value;
        }
        ++value_count;
      }

      auto ds_int =
          ChunkEncoder::encode_segment(new_value_segment, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary});
      segments.emplace_back(ds_int);
    }

    const auto dictionary_encoded_chunk = std::make_shared<Chunk>(segments);
    return dictionary_encoded_chunk;
  }

  static std::vector<std::shared_ptr<Chunk>> get_chunks(
        const std::string file_name,
        const uint32_t row_count,
        const uint32_t column_count,
        const uint32_t count) {

    std::remove(file_name.c_str());
    const auto chunk = create_dictionary_segment_chunk(row_count, column_count);
    std::vector<std::shared_ptr<Chunk>> chunks(count);
    for (auto index = size_t{0}; index < count; ++index) {
      chunks[index] = chunk;
    }

    return chunks;
  }

  static std::vector<std::shared_ptr<Chunk>> map_chunks_from_file(
    std::string file_name,
    uint32_t COLUMN_COUNT,
    FILE_HEADER read_header) {
    auto& sm = Hyrise::get().storage_manager;
    const auto CHUNK_COUNT = sm.get_max_chunk_count_per_file();

    auto mapped_chunks = std::vector<std::shared_ptr<Chunk>>{};
    for (auto index = size_t{0}; index < CHUNK_COUNT; ++index) {
      if (index == 0) {
        mapped_chunks.emplace_back(sm.map_chunk_from_disk(sm.get_file_header_bytes(), file_name, COLUMN_COUNT));
      } else {
        mapped_chunks.emplace_back(
            sm.map_chunk_from_disk(read_header.chunk_offset_ends[index - 1], file_name, COLUMN_COUNT));
      }
    }
    return mapped_chunks;
  }

  static uint64_t accumulate_sum_of_segment(
      std::vector<std::shared_ptr<Chunk>> mapped_chunks,
      uint16_t chunk_index,
      uint16_t segment_index) {

    const auto mapped_dictionary_segment =
      dynamic_pointer_cast<DictionarySegment<int>>(mapped_chunks[chunk_index]->get_segment(ColumnID{segment_index}));
    auto mapped_dict_segment_iterable = create_iterable_from_segment<int>(*mapped_dictionary_segment);

    auto column_sum_of_mapped_chunk = uint64_t{};
    mapped_dict_segment_iterable.with_iterators([&](auto it, auto end) {
      column_sum_of_mapped_chunk = std::accumulate(
          it, end, uint64_t{0},
          [](const auto& accumulator, const auto& currentValue) { return accumulator + currentValue.value(); });
    });

    return column_sum_of_mapped_chunk;
  }

  static uint64_t accumulate_sum_of_segment(std::shared_ptr<Chunk> chunk, uint16_t segment_index) {
    std::vector<std::shared_ptr<Chunk>> chunk_vector { chunk };
    return accumulate_sum_of_segment(chunk_vector, 0, segment_index);
  }

  static std::shared_ptr<Chunk> create_dictionary_segment_chunk_large(const uint32_t row_count,
                                                                      const uint32_t column_count) {
    /*
   * Create a chunk with index-times repeating elements in each segment.
   * Example: in segment 0 every value is unique, in segment 1 every value appears twice, in segment 2 thrice ...
   * Dictionary-encode each segment and return dictionary encoded chunk.
   */
    auto segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};
    for (auto segment_index = uint32_t{0}; segment_index < column_count; ++segment_index) {
      auto new_value_segment = std::make_shared<ValueSegment<int32_t>>(false, ChunkOffset{row_count});

      auto current_value = static_cast<int32_t>(row_count);
      auto value_count = uint32_t{1};  //start 1-indexed to avoid issues with modulo operations
      while (value_count - 1 <
             row_count) {  //as we start 1-indexed we need to adapt while-condition to create row-count many elements
        new_value_segment->append(current_value);
        --current_value;
        ++value_count;
      }

      auto encoded_segment =
          ChunkEncoder::encode_segment(new_value_segment, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary});
      segments.emplace_back(encoded_segment);
    }

    const auto dictionary_encoded_chunk = std::make_shared<Chunk>(segments);
    return dictionary_encoded_chunk;
  }
};

}  // namespace hyrise
