#include <fcntl.h>
#include <sys/mman.h>
#include <fstream>
#include <iostream>
#include <numeric>
#include <vector>
#include "types.hpp"

#include <unistd.h>

#include "storage/chunk_encoder.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/value_segment.hpp"
#include "storage/base_dictionary_segment.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/dictionary_segment/dictionary_segment_iterable.hpp"

using namespace hyrise;  // NOLINT

struct file_header {
  uint16_t storage_format_version_id;
  uint16_t chunk_count;
  std::array<uint32_t, 50> chunk_ids;
  std::array<uint32_t, 50> chunk_offset_ends;
};

std::string fail_and_close_file(const int32_t fd, const std::string& message, const int error_num) {
  close(fd);
  return message + std::strerror(error_num);
}

std::shared_ptr<Chunk> create_dictionary_segment_chunk(const uint32_t row_count, const uint32_t column_count) {
  /*
   * Create a chunk with index-times repeating elements in each segment.
   * Example: in segment 0 every value is unique, in segment 1 every value appears twice, in segment 2 thrice ...
   * Dictionary-encode each segment and return dictionary encoded chunk.
   */

  auto segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};
  const auto num_values = int64_t{column_count * row_count};

  std::cout << "We create a dictionary-encoded chunk with " << column_count << " columns, " << row_count << " rows and thus "
            << num_values << " values." << std::endl;

  for (auto segment_index = uint32_t{0}; segment_index < column_count; ++segment_index) {
    auto new_value_segment = std::make_shared<ValueSegment<int>>();

    auto current_value = int32_t{0};
    auto value_count = uint32_t{1}; //start 1-indexed to avoid issues with modulo operations

    while (value_count - 1 < row_count) { //as we start 1-indexed we need to adapt while-condition to create row-count many elements
      new_value_segment->append(current_value);

      //create segment-index many duplicates of each value in the segment
      if (value_count % (segment_index + 1) == 0) {
        ++current_value;
      }
      ++value_count;
    }

    auto ds_int = ChunkEncoder::encode_segment(new_value_segment, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary});
    segments.emplace_back(ds_int);
  }

  const auto dictionary_encoded_chunk = std::make_shared<Chunk>(segments);

  return dictionary_encoded_chunk;
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

// needed for attribute vector which is stored in a compact manner
void export_compact_vector(std::ofstream& ofstream, const pmr_compact_vector& values) {
  //adapted to uint32_t format of later created map (see comment in `write_dict_segment`)
  export_value(ofstream, static_cast<uint32_t>(values.bits()));
  ofstream.write(reinterpret_cast<const char*>(values.get()), static_cast<int64_t>(values.bytes()));
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

void write_dict_segment(const std::shared_ptr<DictionarySegment<int>> segment, const std::string& filename) {
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

  std::ofstream chunk_file;
  chunk_file.open(filename, std::ios::out | std::ios::binary | std::ios::app);

  // We will later mmap to an uint32_t vector/array. Therefore, we store all metadata points as uint32_t.
  // This wastes up to three bytes of compression per metadata point but makes mapping much easier.
  export_value(chunk_file, static_cast<uint32_t>(segment->dictionary()->size()));
  export_value(chunk_file, static_cast<uint32_t>(segment->attribute_vector()->size()));

  const auto compressed_vector_type_id = static_cast<uint32_t>(infer_compressed_vector_type_id<int>(*segment));
  export_value(chunk_file, compressed_vector_type_id);

  export_values<int>(chunk_file, *segment->dictionary());

  export_compressed_vector(chunk_file, *segment->compressed_vector_type(),
                          *segment->attribute_vector());
  chunk_file.close();
}

void write_chunk(const std::shared_ptr<Chunk> chunk, const std::string& chunk_filename) {
  const auto file_extension = ".bin";
  const auto filename = chunk_filename + file_extension;
  const auto segment_count = chunk->column_count();

  for (auto segment_index = size_t{0}; segment_index < segment_count; ++segment_index) {
    const auto abstract_segment = chunk->get_segment(static_cast<ColumnID>(static_cast<uint16_t>(segment_index)));
    const auto dict_segment = dynamic_pointer_cast<DictionarySegment<int>>(abstract_segment);
    write_dict_segment(dict_segment, filename);
  }
}

std::shared_ptr<Chunk> map_chunk(const std::string& chunk_name, const uint32_t segment_count) {
  auto segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};

  auto fd = int32_t{};
  const auto file_extension = ".bin";
  const auto chunk_filename = chunk_name + file_extension;
  Assert((fd = open(chunk_filename.c_str(), O_RDONLY)) >= 0, fail_and_close_file(fd, "Open error: ", errno));

  const auto offset = off_t{0};

  // As we store a variable number of bytes per segment the easiest solution is to
  // obtain the bytes to mmap via a file system call.
  const auto file_bytes = std::filesystem::file_size(chunk_filename);

  auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, file_bytes, PROT_READ, MAP_PRIVATE, fd, offset));
  Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));
  close(fd);

  madvise(map, file_bytes, MADV_SEQUENTIAL);

  auto currently_mapped_elements = uint32_t{0};
  const auto meta_data_elements = uint32_t{3};
  for (auto segment_index = size_t{0}; segment_index < segment_count; ++segment_index) {
    const auto dictionary_size = map[currently_mapped_elements];
    const auto attribute_vector_size = map[currently_mapped_elements + 1];
    //const auto encoding_type = map[currently_mapped_elements + 2]; //currently unused, see `write_dict_chunk` comment

    // As a first step we don't use the mmap as the underlying data structure of the DictionarySegment but
    // copy in-memory from the mmap to the relevant vectors.
    pmr_vector<int> dictionary_values(dictionary_size);
    memcpy(dictionary_values.data(), map + (currently_mapped_elements + meta_data_elements), dictionary_size * sizeof(int));
    auto dictionary = std::make_shared<pmr_vector<int>>(dictionary_values);

    pmr_vector<uint16_t> attribute_values(attribute_vector_size);
    memcpy(attribute_values.data(), map + (currently_mapped_elements + meta_data_elements + dictionary_size), attribute_vector_size * sizeof(uint16_t));
    auto attribute_vector = std::make_shared<FixedWidthIntegerVector<uint16_t>>(attribute_values);

    const auto dictionary_segment = std::make_shared<DictionarySegment<int>>(dictionary, attribute_vector);
    segments.emplace_back(dynamic_pointer_cast<AbstractSegment>(dictionary_segment));
    currently_mapped_elements += meta_data_elements + dictionary_size + attribute_vector_size / 2;
  }

  const auto chunk = std::make_shared<Chunk>(segments);
  return chunk;
}

//void unmap_chunk(dict_chunk_prototype mapped_chunk, const uint32_t mapped_chunk_bytes) {
//  Assert((munmap(mapped_chunk[0].data(), mapped_chunk_bytes) == 0), "Unmapping failed.");
//}

int main() {
  std::cout << "Playground started." << std::endl;

  auto vs_int = std::make_shared<ValueSegment<int>>();
  vs_int->append(4);
  vs_int->append(6);
  vs_int->append(3);

  auto vs_str = std::make_shared<ValueSegment<pmr_string>>();
  vs_str->append("Hello,");
  vs_str->append("world");
  vs_str->append("!");

  auto ds_int = ChunkEncoder::encode_segment(vs_int, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary});
  auto ds_str = ChunkEncoder::encode_segment(vs_str, DataType::String, SegmentEncodingSpec{EncodingType::FixedStringDictionary});

  pmr_vector<std::shared_ptr<AbstractSegment>> empty_segments;
  empty_segments.push_back(std::make_shared<ValueSegment<int32_t>>());
  empty_segments.push_back(std::make_shared<ValueSegment<pmr_string>>());

  auto chunk = std::make_shared<Chunk>(empty_segments);
  auto chunks = std::vector<std::shared_ptr<Chunk>>{};

  chunks.emplace_back(chunk);

  const auto segment_count = uint32_t{23};
  const auto row_count = uint32_t{65'000};
  const auto dictionary_chunk = create_dictionary_segment_chunk(row_count, segment_count);
  const auto chunk_name = "test_chunk";

  auto chunk_ids = std::array<uint32_t, 50>();
  auto chunk_offset_ends = std::array<uint32_t, 50>();
  for (auto ind = uint32_t{0}; ind < 50; ++ind) {
    chunk_ids[ind] = ind;
    chunk_offset_ends[ind] = ind;
  }

  std::remove("test_chunk.bin"); //remove previously written file

  file_header header;
  header.storage_format_version_id = 1;
  header.chunk_count = 50;
  header.chunk_ids = chunk_ids;
  header.chunk_offset_ends = chunk_offset_ends;

  std::ofstream chunk_file;
  chunk_file.open("test_chunk.bin", std::ios::out | std::ios::binary | std::ios::app);
  chunk_file.write(reinterpret_cast<char*>(&header), sizeof(file_header));
  chunk_file.close();

  write_chunk(dictionary_chunk, chunk_name);
  const auto mapped_chunk = map_chunk(chunk_name, segment_count);

  // compare sum of column 17 in created and mapped chunk
  const auto dict_segment_16 = dynamic_pointer_cast<DictionarySegment<int>>(dictionary_chunk->get_segment(ColumnID{16}));
  auto dict_segment_iterable = create_iterable_from_segment<int>(*dict_segment_16);

  auto column_sum_of_created_chunk = uint64_t{};
  dict_segment_iterable.with_iterators([&](auto it, auto end) {
    column_sum_of_created_chunk = std::accumulate(it, end, uint64_t{0}, [](const auto& accumulator, const auto& currentValue) {
      return accumulator + currentValue.value();
    });
  });

  std::cout << "Sum of column 17 of created chunk: " << column_sum_of_created_chunk << std::endl;

  const auto mapped_dictionary_segment = dynamic_pointer_cast<DictionarySegment<int>>(mapped_chunk->get_segment(ColumnID{16}));
  auto mapped_dict_segment_iterable = create_iterable_from_segment<int>(*mapped_dictionary_segment);

  auto column_sum_of_mapped_chunk = uint64_t{};
  mapped_dict_segment_iterable.with_iterators([&](auto it, auto end) {
    column_sum_of_mapped_chunk = std::accumulate(it, end, uint64_t{0}, [](const auto& accumulator, const auto& currentValue) {
      return accumulator + currentValue.value();
    });
  });

  std::cout << "Sum of column 17 of mapped chunk: " << column_sum_of_mapped_chunk << std::endl;

  // print row 17 of created and mapped chunk
  std::cout << "Row 17 of created chunk: ";
  for (auto column_index = size_t{0}; column_index < segment_count; ++column_index) {
    const auto dict_segment = dynamic_pointer_cast<DictionarySegment<int>>(dictionary_chunk->get_segment(static_cast<ColumnID>(static_cast<uint16_t>(column_index))));
    std::cout << (dict_segment->get_typed_value(ChunkOffset{16})).value() << " ";
  }
  std::cout << std::endl;

  std::cout << "Row 17 of mapped chunk: ";
  for (auto column_index = size_t{0}; column_index < segment_count; ++column_index) {
    const auto dict_segment = dynamic_pointer_cast<DictionarySegment<int>>(mapped_chunk->get_segment(static_cast<ColumnID>(static_cast<uint16_t>(column_index))));
    std::cout << (dict_segment->get_typed_value(ChunkOffset{16})).value() << " ";
  }

  // Todo: Unmapping currently doesn't work, because we copy from the map and don't use underlying storage directly.
  // We either need to get passed a direct map reference or find a way to make DictionarySegments work with std:span.
  // const auto mapped_chunk_bytes = std::filesystem::file_size("test_chunk.bin");
  // unmap_chunk(mapped_chunk, mapped_chunk_bytes);

  return 0;
}
