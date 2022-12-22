#include <fcntl.h>
#include <sys/mman.h>
#include <fstream>
#include <iostream>
#include <numeric>
#include <span>
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

using chunk_prototype = std::vector<std::shared_ptr<ValueSegment<int>>>;
using dict_chunk_prototype = std::vector<std::shared_ptr<DictionarySegment<int>>>;

std::string fail_and_close_file(const int32_t fd, const std::string& message, const int error_num) {
  close(fd);
  return message + std::strerror(error_num);
}

dict_chunk_prototype create_dictionary_segment_chunk(const uint32_t row_count, const uint32_t column_count) {
  auto chunk = chunk_prototype{};
  auto dict_chunk = dict_chunk_prototype{};
  const auto value_count = int64_t{column_count * row_count};

  std::cout << "We create a chunk with " << column_count << " columns, " << row_count << " rows and thus "
            << value_count << " values." << std::endl;
  chunk.reserve(column_count);

  // create columns
  for (auto column_index = uint32_t{0}; column_index < column_count; ++column_index) {
    auto new_value_segment = std::make_shared<ValueSegment<int>>();
    // create row_count many values per segment
    // in first segment every value appears once, in second segment every value appears twice and so on
    auto current_value = int32_t{0};
    auto value_index = uint32_t{1};

    while (value_index < row_count + 1) {

      new_value_segment->append(current_value);

      if (value_index % (column_index + 1) == 0) {
        current_value++;
      }

      value_index++;
    }

    chunk.emplace_back(new_value_segment);
  }

  // dictionary encode segments
  dict_chunk.reserve(column_count);
  for (auto column_index = uint32_t{0}; column_index < column_count; ++column_index) {
    auto segment =
      ChunkEncoder::encode_segment(chunk.at(column_index), DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary});
    auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(segment);
    dict_chunk.emplace_back(dict_segment);
  }

  return dict_chunk;
}

// shameless copy from `binary_writer.cpp`
template <typename T, typename Alloc>
void export_values(std::ofstream& ofstream, const std::vector<T, Alloc>& values) {
  ofstream.write(reinterpret_cast<const char*>(values.data()), values.size() * sizeof(T));
}

template <typename T>
void export_value(std::ofstream& ofstream, const T& value) {
  ofstream.write(reinterpret_cast<const char*>(&value), sizeof(T));
}

// necessary for attribute vector which is stored in a compact manner
void export_compact_vector(std::ofstream& ofstream, const pmr_compact_vector& values) {
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
  std::ofstream column_file;
  column_file.open(filename, std::ios::out | std::ios::binary | std::ios::app);

  //we write
  // bytes_dictionary_vector -> segment->size()
  // len_attribute_vector -> segment->attribute_vector()->size() || segment->unique_values_count()
  // attribute vector compression type
  // dictionary_vector
  // attribute_vector

  export_value(column_file, static_cast<uint32_t>(segment->dictionary()->size()));
  export_value(column_file, static_cast<uint32_t>(segment->attribute_vector()->size()));

  // Write attribute vector compression id
  const auto compressed_vector_type_id = static_cast<uint32_t>(infer_compressed_vector_type_id<int>(*segment));
  export_value(column_file, compressed_vector_type_id);

  export_values<int>(column_file, *segment->dictionary());

  export_compressed_vector(column_file, *segment->compressed_vector_type(),
                          *segment->attribute_vector());


  column_file.close();
}

void write_chunk(const dict_chunk_prototype& chunk, const std::string& chunk_filename) {
  const auto file_extension = ".bin";
  const auto filename = chunk_filename + file_extension;
  const auto column_count = chunk.size();
  for (auto column_index = size_t{0}; column_index < column_count; ++column_index) {
    write_dict_segment(chunk[column_index], filename);
  }
}

dict_chunk_prototype map_chunk(const std::string& chunk_name, const uint32_t column_count) {
  auto mapped_chunk = std::vector<std::span<uint32_t>>();
  auto dict_chunk = dict_chunk_prototype{};

  auto fd = int32_t{};
  const auto file_extension = ".bin";
  const auto chunk_filename = chunk_name + file_extension;
  Assert((fd = open(chunk_filename.c_str(), O_RDONLY)) >= 0, fail_and_close_file(fd, "Open error: ", errno));

  const auto offset = off_t{0};

  const auto file_bytes = std::filesystem::file_size(chunk_filename);

  auto* map = reinterpret_cast<int*>(mmap(NULL, file_bytes, PROT_READ, MAP_PRIVATE, fd, offset));
  Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));
  close(fd);

  madvise(map, file_bytes, MADV_SEQUENTIAL);

  auto currently_mapped_elements = uint32_t{0};
  for (auto column_index = size_t{0}; column_index < column_count; ++column_index) {
    const auto dictionary_size = map[currently_mapped_elements];
    const auto attribute_vector_size = map[currently_mapped_elements + 1];
    //const auto encoding_type = map[currently_mapped_elements + 2];

    // create std::span views on dictionary and attribute vector
    pmr_vector<int> dictionary_values(dictionary_size);
    memcpy(dictionary_values.data(), map + (currently_mapped_elements + 3), dictionary_size * sizeof(int));

    auto dictionary = std::make_shared<pmr_vector<int>>(dictionary_values);

    pmr_vector<uint16_t> attribute_values(attribute_vector_size);
    //file.read(reinterpret_cast<char*>(values.get()), static_cast<int64_t>(values.bytes()))
    memcpy(attribute_values.data(), map + (currently_mapped_elements + 3 + dictionary_size), attribute_vector_size * sizeof(uint16_t));

    auto attribute_vector = std::make_shared<FixedWidthIntegerVector<uint16_t>>(attribute_values);
    dict_chunk.emplace_back(std::make_shared<DictionarySegment<int>>(dictionary, attribute_vector));
    currently_mapped_elements += 3 + dictionary_size + attribute_vector_size / 2;
  }
  return dict_chunk;
}

void unmap_chunk(std::vector<std::span<uint32_t>> mapped_chunk, const uint32_t column_count,
                                           const uint32_t segment_size) {
  const auto chunk_bytes = segment_size * column_count * sizeof(uint32_t);
  Assert((munmap(mapped_chunk[0].data(), chunk_bytes) == 0), "Unmapping failed.");
}

int main() {
  std::cout << "Playground started." << std::endl;

  const auto column_count = uint32_t{23};
  const auto row_count = uint32_t{65'000};
  auto dictionary_chunk = create_dictionary_segment_chunk(row_count, column_count);
  const auto chunk_name = "test_chunk";

// TODO: Make file removal before writing prettier.
  std::remove("test_chunk.bin");
  write_chunk(dictionary_chunk, chunk_name);

  std::cout << "Finished writing." << std::endl;

  std::cout << "Start reading." << std::endl;

  const auto mapped_chunk = map_chunk(chunk_name, column_count);

// calculate sum of column 17
  auto dict_segment_iterable = create_iterable_from_segment<int>(*dictionary_chunk[16]);

  auto column_sum_of_created_chunk = uint64_t{};
  dict_segment_iterable.with_iterators([&](auto it, auto end) {
    column_sum_of_created_chunk = std::accumulate(it, end, uint64_t{0}, [](const auto& accumulator, const auto& currentValue) {
      return accumulator + currentValue.value();
    });
  });

  std::cout << "Sum of column 17 of created chunk: " << column_sum_of_created_chunk << std::endl;

  auto mapped_dict_segment_iterable = create_iterable_from_segment<int>(*mapped_chunk[16]);

  auto column_sum_of_mapped_chunk = uint64_t{};
  mapped_dict_segment_iterable.with_iterators([&](auto it, auto end) {
    column_sum_of_mapped_chunk = std::accumulate(it, end, uint64_t{0}, [](const auto& accumulator, const auto& currentValue) {
      return accumulator + currentValue.value();
    });
  });

  std::cout << "Sum of column 17 of mapped chunk: " << column_sum_of_mapped_chunk << std::endl;

  // print row 17

  std::cout << "Row 17 of created chunk: ";
  for (auto column_index = size_t{0}; column_index < column_count; ++column_index) {
    std::cout << (dictionary_chunk[column_index]->get_typed_value(ChunkOffset{16})).value() << " ";
  }
  std::cout << std::endl;

  std::cout << "Row 17 of mapped chunk: ";
  for (auto column_index = size_t{0}; column_index < column_count; ++column_index) {
    std::cout << (mapped_chunk[column_index]->get_typed_value(ChunkOffset{16})).value() << " ";
  }

//  unmap_chunk(mapped_chunk, column_count, row_count);

  return 0;
}
