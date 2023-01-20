#include <fstream>

#include "types.hpp"

using namespace hyrise;  // NOLINT

struct file_format {
  uint16_t storage_format_version_id;
  uint8_t chunk_count;
  std::vector<ChunkID> chunk_ids;
  std::vector<ChunkOffset> chunk_offset_ends;
  std::vector<char> chunks;
};

size_t get_file_size(std::string filename) {
  std::ifstream in_file(filename, std::ios::binary);
  Assert(in_file, "File does not exist you dump fuck!");
  in_file.seekg(0, std::ios::end);
  return in_file.tellg();
}

int main() {
  file_format test;
  test.storage_format_version_id = 65535;
  test.chunk_count = 7;
  test.chunks.emplace_back('i');
  test.chunks.emplace_back('i');
  test.chunks.emplace_back('i');
  test.chunks.emplace_back('i');
  test.chunks.emplace_back('i');
  test.chunks.emplace_back('i');
  test.chunks.emplace_back('i');
  test.chunk_ids.emplace_back(0);
  test.chunk_ids.emplace_back(1);
  test.chunk_ids.emplace_back(2);
  test.chunk_ids.emplace_back(3);
  test.chunk_ids.emplace_back(4);
  test.chunk_ids.emplace_back(5);
  test.chunk_ids.emplace_back(6);
  test.chunk_offset_ends.emplace_back(42);

  std::ofstream wf("test.bin", std::ios::out | std::ios::binary);
  Assert(wf, "Cannot open file!");

  wf.write((char*) &test.storage_format_version_id, sizeof(test.storage_format_version_id));
  wf.write((char*) &test.chunk_count, sizeof(test.chunk_count));
  wf.write((char*) test.chunk_ids.data(), sizeof(test.chunk_ids[0]) * test.chunk_ids.size());
  wf.write((char*) test.chunk_offset_ends.data(), sizeof(test.chunk_offset_ends[0] * test.chunk_offset_ends.size()));
  wf.write((char*) test.chunks.data(), sizeof(test.chunks[0]) * test.chunks.size());
  wf.close();

  std::ifstream rf("test.bin", std::ios::out | std::ios::binary);
  Assert(rf, "Poooo");

  const auto fsize = get_file_size("test.bin");
  char* buffer = (char*)malloc(fsize);
  (void)buffer;
  rf.read((char*)buffer, fsize);


  for (size_t i = 0; i < fsize; ++i) {
    std::cout << buffer[i];
  }

  auto x = uint16_t{0};
  auto y = uint8_t{0};

  memcpy(&x, &buffer[0], sizeof(x));
  memcpy(&y, &buffer[0], sizeof(y));

  std::cout << x << "+++" << fsize << std::endl;


  // auto fformat = dynamic_cast<file_format*>(buffer);

  // std::cout << fsize << "+++" << sizeof(file_format) << std::endl;

  // std::cout << loaded_data.storage_format_version_id << std::endl;// "+++" << loaded_data.chunk_count << std::endl;


  return 0;
}
