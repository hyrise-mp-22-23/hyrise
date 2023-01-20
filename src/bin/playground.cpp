#include <fstream>

#include "types.hpp"
#include "storage/value_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"



using namespace hyrise;  // NOLINT

struct file_format {
  uint16_t storage_format_version_id;
  uint8_t chunk_count;
  std::array<ChunkID, 50> chunk_ids;
  std::array<ChunkOffset, 50> chunk_offset_ends;
};

struct chunk_header {
  ColumnID segment_count;
  ChunkOffset row_count;
  std::vector<DataType> data_types;
};

chunk_header create_chunk_header(const std::shared_ptr<Chunk> chunk) {
  auto result = chunk_header{};

  result.segment_count = chunk->column_count();
  result.row_count = chunk->size();

  for (auto chunk_index = ColumnID{0}; chunk_index < result.segment_count; ++chunk_index) {
    const auto segment = chunk->get_segment(chunk_index);
    result.data_types.emplace_back(segment->data_type());
  }

  return result;
}


size_t get_file_size(std::string filename) {
  std::ifstream in_file(filename, std::ios::binary);
  Assert(in_file, "File does not exist you lovely human being!");
  in_file.seekg(0, std::ios::end);
  return in_file.tellg();
}

file_format create_header_for_chunk(const std::vector<std::shared_ptr<Chunk>>& chunks) {
  auto result = file_format{};
  
  result.storage_format_version_id = 0;
  result.chunk_count = chunks.size();

  for (auto chunk_index = size_t{0}; chunk_index < result.chunk_count; ++chunk_index) {
    result.chunk_ids[chunk_index] = chunk_index;
  }

  return result;
}

int main() {
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

  file_format test;
  std::cout << sizeof(test.chunk_ids) << std::endl;

  // std::ofstream wf("test.bin", std::ios::out | std::ios::binary);
  // // Assert(wf, "Cannot open file!");

  // auto char_chunk = (char*) &chunk;
  // wf.write(char_chunk, );

  // wf.write((char*) &test.storage_format_version_id, sizeof(test.storage_format_version_id));
  // wf.write((char*) &test.chunk_count, sizeof(test.chunk_count));
  // wf.write((char*) test.chunk_ids.data(), sizeof(test.chunk_ids[0]) * test.chunk_ids.size());
  // wf.write((char*) test.chunk_offset_ends.data(), sizeof(test.chunk_offset_ends[0] * test.chunk_offset_ends.size()));
  // wf.write((char*) test.chunks.data(), sizeof(test.chunks[0]) * test.chunks.size());
  // wf.close();

  // std::ifstream rf("test.bin", std::ios::out | std::ios::binary);
  // Assert(rf, "Poooo");

  // const auto fsize = get_file_size("test.bin");
  // char* buffer = (char*)malloc(fsize);
  // (void)buffer;
  // rf.read((char*)buffer, fsize);


  // for (size_t i = 0; i < fsize; ++i) {
  //   std::cout << buffer[i];
  // }

  // auto x = uint16_t{0};
  // auto y = uint8_t{0};

  // memcpy(&x, &buffer[0], sizeof(x));
  // memcpy(&y, &buffer[0], sizeof(y));

  // std::cout << x << "+++" << fsize << std::endl;


  // auto fformat = dynamic_cast<file_format*>(buffer);

  // std::cout << fsize << "+++" << sizeof(file_format) << std::endl;

  // std::cout << loaded_data.storage_format_version_id << std::endl;// "+++" << loaded_data.chunk_count << std::endl;


  return 0;
}
