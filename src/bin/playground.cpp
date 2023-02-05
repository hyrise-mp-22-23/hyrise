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
const auto COLUMN_COUNT = uint32_t{23};
const auto ROW_COUNT = uint32_t{65'000};
const auto FILENAME = "z_binary_test.bin";
const auto CREATE_COUNT = uint32_t{4};

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

std::string fail_and_close_file(const int32_t fd, const std::string& message, const int error_num) {
  close(fd);
  return message + std::strerror(error_num);
}

void print_file_header(file_header header) {
  std::cout << "ID: " << header.storage_format_version_id << " Count: " << header.chunk_count << std::endl;
  for (auto header_index = size_t{0}; header_index < header.chunk_count; ++header_index) {
    std::cout << "ChunkID: " << header.chunk_ids[header_index] << " ChunkOffsetEnd: " << header.chunk_offset_ends[header_index] << std::endl;
  }
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

    auto current_value = int32_t{65'000};
    auto value_count = uint32_t{1}; //start 1-indexed to avoid issues with modulo operations

    while (value_count - 1 < row_count) { //as we start 1-indexed we need to adapt while-condition to create row-count many elements
      new_value_segment->append(current_value);

      //create segment-index many duplicates of each value in the segment
      if (value_count % (segment_index + 1) == 0) {
        --current_value;
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
  //TODO: Update comment.
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

  //TODO: Should this be continued?
  // We will later mmap to an uint32_t vector/array. Therefore, we store all metadata points as uint32_t.
  // This wastes up to three bytes of compression per metadata point but makes mapping much easier.
  export_value(chunk_file, static_cast<uint32_t>(segment->dictionary()->size()));
  export_value(chunk_file, static_cast<uint32_t>(segment->attribute_vector()->size()));

  // std::cout << "DictionarySize: " << segment->dictionary()->size() << " AttributeVectorSize: " << segment->attribute_vector()->size() << std::endl;

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

  auto offset_end = static_cast<uint32_t>(4 + 4 * segment_count);
  for (auto segment_index = size_t{0}; segment_index < segment_count; ++segment_index) {
    // 4 Byte Dictionary Size + 4 Byte Attribute Vector Size + 4 Compressed Vector Type ID
    offset_end += 12;

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

    segment_offset_ends[segment_index] = offset_end;
  }
  return segment_offset_ends;
}

void write_chunk_to_disk(const std::shared_ptr<Chunk> chunk, const std::string& filename) {
  //TODO: Why open a new filestream for each chunk, when we are writing multiple chunks?
  std::ofstream chunk_file;
  chunk_file.open(filename, std::ios::out | std::ios::binary | std::ios::app);

  chunk_header header;
  header.row_count = chunk->size();
  //TODO: Remove double calculation of segment_offsets. Gets called in generate_file_header, too.
  header.segment_offset_ends = generate_segment_offset_ends(chunk);

  export_value(chunk_file, header.row_count);
  //TODO: Use export_values()?
  for (const auto segment_offset_end : header.segment_offset_ends) {
    export_value(chunk_file, segment_offset_end);
  }
  //TODO: Why does this get closed when we will continue writing?
  chunk_file.close();

  const auto segment_count = chunk->column_count();
  for (auto segment_index = size_t{0}; segment_index < segment_count; ++segment_index) {
    const auto abstract_segment = chunk->get_segment(static_cast<ColumnID>(static_cast<uint16_t>(segment_index)));
    const auto dict_segment = dynamic_pointer_cast<DictionarySegment<int>>(abstract_segment);

    write_dict_segment_to_disk(dict_segment, filename);
  }
}

chunk_header read_chunk_header(const std::string filename, const uint32_t segment_count, const uint32_t chunk_offset_begin) {
  //TODO: Remove need to map the whole file.
  chunk_header header;
  const auto map_index = chunk_offset_begin / 4;

  auto fd = int32_t{};
  Assert((fd = open(filename.c_str(), O_RDONLY)) >= 0, fail_and_close_file(fd, "Open error: ", errno));

  const auto file_bytes = std::filesystem::file_size(FILENAME);
  auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, file_bytes, PROT_READ, MAP_PRIVATE, fd, off_t{0}));
  Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));
  close(fd);

  header.row_count = map[map_index];

  for (auto header_index = size_t{1}; header_index < segment_count + 1; ++header_index) {
    header.segment_offset_ends.emplace_back(map[header_index + map_index]);
  }

  return header;
}

std::shared_ptr<Chunk> map_chunk_from_disk(const uint32_t chunk_offset_end) {
  auto segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};

  auto fd = int32_t{};
  Assert((fd = open(FILENAME, O_RDONLY)) >= 0, fail_and_close_file(fd, "Open error: ", errno));

  // As we store a variable number of bytes per segment the easiest solution is to
  // obtain the bytes to mmap via a file system call.
  const auto file_bytes = std::filesystem::file_size(FILENAME);

  //TODO: Remove unneccesary map on whole file
  auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, file_bytes, PROT_READ, MAP_PRIVATE, fd, off_t{0}));
  Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));
  close(fd);

  const auto header = read_chunk_header(FILENAME, COLUMN_COUNT, chunk_offset_end);

  //TODO: Remove magic divisions by 4
  const auto header_offset = chunk_offset_end / 4;


  for (auto segment_index = size_t{0}; segment_index < COLUMN_COUNT; ++segment_index) {
    auto segment_offset_end = uint32_t{4 + COLUMN_COUNT * 4};
    if (segment_index > 0) {
      segment_offset_end = header.segment_offset_ends[segment_index - 1];
    }

    const auto dictionary_size = map[header_offset + segment_offset_end / 4];
    const auto attribute_vector_size = map[header_offset + segment_offset_end / 4 + 1];
    // const auto encoding_type = map[header_offset + segment_offset_end / 4 + 2];
  
    auto dictionary_values = pmr_vector<int32_t>(dictionary_size);
    memcpy(dictionary_values.data(), &map[header_offset + segment_offset_end / 4 + 3], dictionary_size * sizeof(uint32_t));
    auto dictionary = std::make_shared<pmr_vector<int32_t>>(dictionary_values);

    auto attribute_values = pmr_vector<uint16_t>(attribute_vector_size);
    memcpy(attribute_values.data(), &map[header_offset + segment_offset_end / 4 + 3 + dictionary_size], attribute_vector_size * sizeof(uint16_t));
    auto attribute_vector = std::make_shared<FixedWidthIntegerVector<uint16_t>>(attribute_values);

    const auto dictionary_segment = std::make_shared<DictionarySegment<int>>(dictionary, attribute_vector);


    segments.emplace_back(dynamic_pointer_cast<AbstractSegment>(dictionary_segment));
  }

  const auto chunk = std::make_shared<Chunk>(segments);
  return chunk;
}


std::array<uint32_t, CHUNK_COUNT> generate_chunk_offset_ends(std::vector<std::shared_ptr<Chunk>> chunks) {
  auto chunk_offset_ends = std::array<uint32_t, CHUNK_COUNT>();
  auto offset = uint32_t{sizeof(file_header)};

  for (auto index = uint32_t{0}; index < chunks.size(); ++index) {
    const auto segment_offsets = generate_segment_offset_ends(chunks[index]);
    offset += segment_offsets.back();
    // offset += 96;   // WHERE THE HECK COME THESE BYTES FROM???
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

file_header generate_file_header(std::vector<std::shared_ptr<Chunk>> chunks) {
  file_header file_header;

  file_header.storage_format_version_id = 2;
  file_header.chunk_count = static_cast<uint16_t>(chunks.size());
  file_header.chunk_ids = generate_chunk_ids();
  file_header.chunk_offset_ends = generate_chunk_offset_ends(chunks);

  return file_header;
}

uint32_t combined(const uint16_t low, const uint16_t high) {
    return (static_cast<uint32_t>(high) << 16) | ((static_cast<uint32_t>(low)) & 0xFFFF);
}

file_header read_file_header(std::string filename) {
  file_header file_header;

  auto fd = int32_t{};
  Assert((fd = open(filename.c_str(), O_RDONLY)) >= 0, fail_and_close_file(fd, "Open error: ", errno));
  auto* map = reinterpret_cast<uint16_t*>(mmap(NULL, sizeof(file_header), PROT_READ, MAP_PRIVATE, fd, off_t{0}));
  Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));
  close(fd);

  file_header.storage_format_version_id = map[0];
  file_header.chunk_count = map[1];

  for (auto header_index = size_t{0}; header_index < file_header.chunk_count; ++header_index) {
    file_header.chunk_ids[header_index] = combined(map[2 + header_index * 2 ], map[2 + header_index * 2 + 1]);
    file_header.chunk_offset_ends[header_index] = combined(map[2 + CHUNK_COUNT * 2 + header_index * 2], map[2 + CHUNK_COUNT * 2 + header_index * 2 + 1]);
  }

  return file_header;
}

int main() {
  std::cout << "Playground started." << std::endl;

  auto chunks = std::vector<std::shared_ptr<Chunk>>{};

  for (auto index = size_t{0}; index < CREATE_COUNT; ++index) {
    chunks.emplace_back(create_dictionary_segment_chunk(ROW_COUNT, COLUMN_COUNT));
  }

  std::remove(FILENAME); // Remove previously written file

  file_header header = generate_file_header(chunks);

  //TODO: Why close filestream?
  std::ofstream chunk_file;
  chunk_file.open(FILENAME, std::ios::out | std::ios::binary | std::ios::app);
  chunk_file.write(reinterpret_cast<char*>(&header), sizeof(file_header));
  chunk_file.close();

  std::cout << "File header written."  << std::endl;

  //TODO: Why read header again before the chunk file is written completely?
  file_header read_header = read_file_header(FILENAME);

  for (auto index = uint32_t{0}; index < chunks.size(); ++index) {
    write_chunk_to_disk(chunks[index], FILENAME);
  }
  std::cout << "Chunks written."  << std::endl;

  auto mapped_chunks = std::vector<std::shared_ptr<Chunk>>{};
  for (auto index = size_t{0}; index < CREATE_COUNT; ++index) {
    if (index == 0) {
      mapped_chunks.emplace_back(map_chunk_from_disk(sizeof(file_header)));
    } else {
      mapped_chunks.emplace_back(map_chunk_from_disk(read_header.chunk_offset_ends[index - 1]));
    }
  }

  // compare sum of column 17 in created and mapped chunk
  const auto dict_segment_16 = dynamic_pointer_cast<DictionarySegment<int>>(chunks[0]->get_segment(ColumnID{16}));
  auto dict_segment_iterable = create_iterable_from_segment<int>(*dict_segment_16);

  auto column_sum_of_created_chunk = uint64_t{};
  dict_segment_iterable.with_iterators([&](auto it, auto end) {
    column_sum_of_created_chunk = std::accumulate(it, end, uint64_t{0}, [](const auto& accumulator, const auto& currentValue) {
      return accumulator + currentValue.value();
    });
  });

  std::cout << "Sum of column 17 of created chunk: " << column_sum_of_created_chunk << std::endl;

  const auto mapped_dictionary_segment = dynamic_pointer_cast<DictionarySegment<int>>(mapped_chunks[3]->get_segment(ColumnID{16}));
  auto mapped_dict_segment_iterable = create_iterable_from_segment<int>(*mapped_dictionary_segment);

  auto column_sum_of_mapped_chunk = uint64_t{};
  mapped_dict_segment_iterable.with_iterators([&](auto it, auto end) {
    column_sum_of_mapped_chunk = std::accumulate(it, end, uint64_t{0}, [](const auto& accumulator, const auto& currentValue) {
      return accumulator + currentValue.value();
    });
  });

  std::cout << "Sum of column 17 of third mapped chunk: " << column_sum_of_mapped_chunk << std::endl;

  std::cout << "Col 0 of created chunk: ";
  const auto original_dict_segment = dynamic_pointer_cast<DictionarySegment<int>>(chunks[0]->get_segment(ColumnID{0}));
  for (auto row_index = ChunkOffset{0}; row_index < 20; ++row_index) {
    std::cout << (original_dict_segment->get_typed_value(row_index)).value() << " ";
  }
  std::cout << std::endl;

  std::cout << "Col 0 of third mapped chunk: ";
  const auto original_dict_segment1 = dynamic_pointer_cast<DictionarySegment<int>>(mapped_chunks[3]->get_segment(ColumnID{0}));
  for (auto row_index = ChunkOffset{0}; row_index < 20; ++row_index) {
    std::cout << (original_dict_segment1->get_typed_value(row_index)).value() << " ";
  }
  std::cout << std::endl;

  return 0;
}
