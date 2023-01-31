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

static const int CHUNK_COUNT = 50;
// header of each segment: #Elem, #ElemInAttVec, CompressionType
const auto SEGMENT_META_DATA_ELEMENT_COUNT = uint32_t{3};
const auto SEGMENT_COUNT = uint32_t{23};
const auto ROW_COUNT = uint32_t{65'000};

using namespace hyrise;  // NOLINT

struct file_header {
  uint16_t storage_format_version_id;
  uint16_t chunk_count;
  std::array<uint32_t, CHUNK_COUNT> chunk_ids;
  std::array<uint32_t, CHUNK_COUNT> chunk_offset_ends;
};

struct chunk_header {
  uint32_t row_count;
  std::vector<uint32_t> segment_offset_ends;
};

size_t getChunkMetaDataElementSize(uint32_t segment_count);

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
    auto new_value_segment = std::make_shared<ValueSegment<int32_t>>();

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
  //adapted to uint32_t format of later created map (see comment in `write_dict_segment_to_disk`)
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

void write_dict_segment_to_disk(const std::shared_ptr<DictionarySegment<int>> segment, const std::string& filename) {
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

  export_values<int32_t>(chunk_file, *segment->dictionary());

  export_compressed_vector(chunk_file, *segment->compressed_vector_type(),
                          *segment->attribute_vector());
  chunk_file.close();
}

std::vector<uint32_t> generate_segment_offset_ends(const std::shared_ptr<Chunk> chunk) {
  const auto segment_count = chunk->column_count();
  auto segment_offset_ends = std::vector<uint32_t>(segment_count);

  for (auto segment_index = size_t{0}; segment_index < segment_count; ++segment_index) {
    // 4 Byte Dictionary Size + 4 Byte Attribute Vector Size + 4 Compressed Vector Type ID
    auto offset_end = uint32_t{12};

    const auto abstract_segment = chunk->get_segment(static_cast<ColumnID>(static_cast<uint16_t>(segment_index)));
    const auto dict_segment = dynamic_pointer_cast<DictionarySegment<int>>(abstract_segment);

    offset_end += dict_segment->dictionary()->size() * 4;

    const auto attribute_vector = dict_segment->attribute_vector();
    const auto attribute_vector_type = attribute_vector->type();

    switch (attribute_vector_type) {
      case CompressedVectorType::FixedWidthInteger4Byte:
        offset_end += attribute_vector->size() * 4;
        break;
      case CompressedVectorType::FixedWidthInteger2Byte:
        offset_end += attribute_vector->size() * 2;
        break;
      case CompressedVectorType::FixedWidthInteger1Byte:
        offset_end += attribute_vector->size() * 1;
        break;
      case CompressedVectorType::BitPacking:
        offset_end += 4;
        offset_end += dynamic_cast<const BitPackingVector&>(*attribute_vector).data().bytes();
        break;
      default:
        Fail("Any other type should have been caught before.");
    }

    if(segment_index == 0){
      segment_offset_ends[segment_index] = offset_end;
    } else {
      segment_offset_ends[segment_index] = segment_offset_ends[segment_index-1] + offset_end;
    }
  }
  return segment_offset_ends;
}

void write_chunk_to_disk(const std::shared_ptr<Chunk> chunk, const std::string& chunk_filename) {
  const auto file_extension = ".bin";
  const auto filename = chunk_filename + file_extension;
  const auto segment_count = chunk->column_count();
  const auto prior_filesize = std::filesystem::file_size(filename);

  auto offset_ends = std::vector<uint32_t>(segment_count);

  const auto segment_offset_ends = generate_segment_offset_ends(chunk);
  std::ofstream chunk_file;
  chunk_file.open(filename, std::ios::out | std::ios::binary | std::ios::app);
  export_value(chunk_file, chunk->size());
  for (auto offset : segment_offset_ends) {
    export_value(chunk_file, prior_filesize + offset);
  }
  chunk_file.close();

  for (auto segment_index = size_t{0}; segment_index < segment_count; ++segment_index) {
    const auto abstract_segment = chunk->get_segment(static_cast<ColumnID>(static_cast<uint16_t>(segment_index)));
    const auto dict_segment = dynamic_pointer_cast<DictionarySegment<int>>(abstract_segment);
    write_dict_segment_to_disk(dict_segment, filename);
  }
}

/*
  This mmap accesses data via 4 Byte Steps (size of uint32_t). Therefore, we need to convert the
  offset to the correct position in the file. This is done by subtracting the header size (51 uint32_t's)
  and adding the chunk index (which starts at 1). If we're at first chunk, we skip the chunk metadata.
*/
uint32_t get_offset_for_chunk(uint32_t* map, const uint32_t chunk_index) {
  if(chunk_index == uint32_t{1}) return uint32_t{101};
  const auto header_size = uint32_t{51};
  std::cout << "header size: " << header_size << std::endl;
  const auto offset_position = header_size + chunk_index - 1;
  std::cout << "Access at index: " << offset_position << std::endl;
  const auto chunk_offset = *(map + offset_position);
  std::cout << "Chunk offset: " << chunk_offset << std::endl;

  return chunk_offset;
}

std::shared_ptr<Chunk> map_chunk_from_disk(const uint32_t chunk_id, uint32_t segment_count) {
  auto segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};

  auto fd = int32_t{};
  auto chunk_filename = "test_chunk.bin";
  Assert((fd = open(chunk_filename, O_RDONLY)) >= 0, fail_and_close_file(fd, "Open error: ", errno));

  const auto offset = off_t{0};

  // As we store a variable number of bytes per segment the easiest solution is to
  // obtain the bytes to mmap via a file system call.
  const auto file_bytes = std::filesystem::file_size(chunk_filename);

  auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, file_bytes, PROT_READ, MAP_PRIVATE, fd, offset));
  Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));
  close(fd);

  const auto chunk_offset = get_offset_for_chunk(map, chunk_id);
  madvise(map, file_bytes, MADV_SEQUENTIAL);
  const auto chunk_meta_data_element_size = getChunkMetaDataElementSize(segment_count);

  auto currently_mapped_elements = static_cast<uint32_t>(chunk_meta_data_element_size);

  for (auto segment_index = size_t{0}; segment_index < segment_count; ++segment_index) {
    const auto dictionary_size = map[chunk_offset + currently_mapped_elements - 1];
    const auto attribute_vector_size = map[chunk_offset + currently_mapped_elements];
    //const auto encoding_type = map[currently_mapped_elements + 2]; //currently unused, see `write_dict_chunk` comment
    // copy in-memory from the mmap to the relevant vectors.
    pmr_vector<int> dictionary_values(dictionary_size);
    memcpy(dictionary_values.data(), map + chunk_offset + (currently_mapped_elements + SEGMENT_META_DATA_ELEMENT_COUNT) - 1, dictionary_size * sizeof(int));
    auto dictionary = std::make_shared<pmr_vector<int>>(dictionary_values);

    pmr_vector<uint16_t> attribute_values(attribute_vector_size);
    memcpy(attribute_values.data(), map + chunk_offset + (currently_mapped_elements + SEGMENT_META_DATA_ELEMENT_COUNT + dictionary_size) - 1, attribute_vector_size * sizeof(uint16_t));
    auto attribute_vector = std::make_shared<FixedWidthIntegerVector<uint16_t>>(attribute_values);

    const auto dictionary_segment = std::make_shared<DictionarySegment<int>>(dictionary, attribute_vector);
    segments.emplace_back(dynamic_pointer_cast<AbstractSegment>(dictionary_segment));
    currently_mapped_elements += SEGMENT_META_DATA_ELEMENT_COUNT + dictionary_size + attribute_vector_size / 2;
  }
  const auto chunk = std::make_shared<Chunk>(segments);
  return chunk;
}

size_t getChunkMetaDataElementSize(uint32_t segment_count) {
  // for each chunk we have the row count and the n segment offsets
  return (1 * sizeof(uint16_t)) + (segment_count * 2);
}

//void unmap_chunk(dict_chunk_prototype mapped_chunk, const uint32_t mapped_chunk_bytes) {
//  Assert((munmap(mapped_chunk[0].data(), mapped_chunk_bytes) == 0), "Unmapping failed.");
//}

std::shared_ptr<Chunk> setupEmptyChunk(){
  pmr_vector<std::shared_ptr<AbstractSegment>> empty_segments;
  empty_segments.push_back(std::make_shared<ValueSegment<int32_t>>());
  empty_segments.push_back(std::make_shared<ValueSegment<pmr_string>>());

  auto chunk = std::make_shared<Chunk>(empty_segments);
  return chunk;
}

std::array<uint32_t, CHUNK_COUNT> generate_chunk_offsets(const std::shared_ptr<Chunk> dictionary_chunk){
  auto chunk_offset_ends = std::array<uint32_t, CHUNK_COUNT>();
  auto offset = uint32_t{sizeof(file_header)};

  for (auto index = uint32_t{0}; index < CHUNK_COUNT; ++index) {
    // For efficiency, this could be taken out of the loop, but in reality,
    // we would not have the same chunk written 50 times.
    const auto segment_offsets = generate_segment_offset_ends(dictionary_chunk);
    const auto chunk_size = uint32_t{segment_offsets.back()} + 1;
    offset += chunk_size;
    chunk_offset_ends[index] = offset;
  }

  return chunk_offset_ends;
}

std::array<uint32_t, CHUNK_COUNT> generate_chunk_ids(){
  auto chunk_ids = std::array<uint32_t, CHUNK_COUNT>();

  for (auto index = uint32_t{0}; index < CHUNK_COUNT; ++index) {
    chunk_ids[index] = index;
  }

  return chunk_ids;
}

int main() {
  std::cout << "Playground started." << std::endl;

  auto chunks = std::vector<std::shared_ptr<Chunk>>{};
  chunks.emplace_back(setupEmptyChunk());

  const auto dictionary_chunk = create_dictionary_segment_chunk(ROW_COUNT, SEGMENT_COUNT);

  std::remove("test_chunk.bin"); //remove previously written file

  file_header header;
  header.storage_format_version_id = 1;
  header.chunk_count = CHUNK_COUNT;
  header.chunk_ids = generate_chunk_ids();
  header.chunk_offset_ends = generate_chunk_offsets(dictionary_chunk);

  std::ofstream chunk_file;
  chunk_file.open("test_chunk.bin", std::ios::out | std::ios::binary | std::ios::app);
  chunk_file.write(reinterpret_cast<char*>(&header), sizeof(file_header));
  chunk_file.close();

  std::cout << "Headers written"  << std::endl;

  const auto chunk_name = "test_chunk";
  for (auto index = uint32_t{0}; index < CHUNK_COUNT; ++index) {
    write_chunk_to_disk(dictionary_chunk, chunk_name);
  }

  std::cout << "Chunks written"  << std::endl;

  std::shared_ptr<Chunk> mapped_chunk;
  for (auto index = uint32_t{0}; index < CHUNK_COUNT; ++index) {
    mapped_chunk = map_chunk_from_disk(index + 1, SEGMENT_COUNT);
  }

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
  for (auto column_index = size_t{0}; column_index < SEGMENT_COUNT; ++column_index) {
    const auto dict_segment = dynamic_pointer_cast<DictionarySegment<int>>(dictionary_chunk->get_segment(static_cast<ColumnID>(static_cast<uint16_t>(column_index))));
    std::cout << (dict_segment->get_typed_value(ChunkOffset{16})).value() << " ";
  }
  std::cout << std::endl;

  std::cout << "Row 17 of mapped chunk: ";
  for (auto column_index = size_t{0}; column_index < SEGMENT_COUNT; ++column_index) {
    const auto dict_segment = dynamic_pointer_cast<DictionarySegment<int>>(mapped_chunk->get_segment(static_cast<ColumnID>(static_cast<uint16_t>(column_index))));
    std::cout << (dict_segment->get_typed_value(ChunkOffset{16})).value() << " ";
  }

  // Todo: Unmapping currently doesn't work, because we copy from the map and don't use underlying storage directly.
  // We either need to get passed a direct map reference or find a way to make DictionarySegments work with std:span.
  // const auto mapped_chunk_bytes = std::filesystem::file_size("test_chunk.bin");
  // unmap_chunk(mapped_chunk, mapped_chunk_bytes);

  return 0;
}
