#include <fcntl.h>
#include <sys/mman.h>
#include <fstream>
#include <iostream>
#include <numeric>
#include <span>
#include <vector>
#include "types.hpp"

#include <unistd.h>

using namespace hyrise;  // NOLINT

#define chunk_prototype std::vector<std::shared_ptr<std::vector<uint32_t>>>

std::string fail_and_close_file(const int32_t fd, const std::string& message, const int error_num) {
  close(fd);
  return message + std::strerror(error_num);
}

chunk_prototype create_chunk(const uint32_t row_count, const uint32_t column_count) {
  auto chunk = chunk_prototype{};
  const auto value_count = column_count * row_count;

  std::cout << "We create a chunk with " << column_count << " columns, " << row_count << " rows and thus "
            << value_count << " values." << std::endl;
  chunk.reserve(column_count);

  // create columns
  for (auto index = uint32_t{0}; index < column_count; ++index) {
    auto new_column = std::vector<uint32_t>{};
    new_column.reserve(row_count);
    chunk.emplace_back(std::make_shared<std::vector<uint32_t>>(new_column));
  }

  // create rows / insert values
  for (auto index = uint64_t{0}; index < value_count; ++index) {
    const auto column_index = index % column_count;
    auto column = chunk.at(column_index);
    column->emplace_back(index);
  }

  return chunk;
}

void write_segment(const std::shared_ptr<std::vector<uint32_t>> segment, const std::string& filename) {
  std::ofstream column_file;
  column_file.open(filename, std::ios::out | std::ios::binary | std::ios::app);
  column_file.write(reinterpret_cast<char*>(std::data(*segment)), segment->size() * sizeof(uint32_t));
  column_file.close();
}

void write_chunk(const chunk_prototype& chunk, const std::string& chunk_filename) {
  const auto file_extension = ".bin";
  const auto filename = chunk_filename + file_extension;
  const auto column_count = chunk.size();
  for (auto column_index = size_t{0}; column_index < column_count; ++column_index) {
    write_segment(chunk[column_index], filename);
  }
}

std::vector<std::span<uint32_t>> map_chunk(const std::string& chunk_name, const uint32_t column_count,
                                           const uint32_t segment_size) {
  auto mapped_chunk = std::vector<std::span<uint32_t>>();

  auto fd = int32_t{};
  const auto file_extension = ".bin";
  const auto chunk_filename = chunk_name + file_extension;
  Assert((fd = open(chunk_filename.c_str(), O_RDONLY)) >= 0, fail_and_close_file(fd, "Open error: ", errno));

  const auto offset = off_t{0};
  const auto chunk_bytes = segment_size * column_count * sizeof(uint32_t);

  auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, chunk_bytes, PROT_READ, MAP_PRIVATE, fd, offset));
  Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));
  close(fd);

  madvise(map, chunk_bytes, MADV_SEQUENTIAL);

  for (auto column_index = size_t{0}; column_index < column_count; ++column_index) {
    // create std::span view on map
    auto map_span_view = std::span{map + (column_index * segment_size), segment_size};
    mapped_chunk.emplace_back(map_span_view);
  }
  return mapped_chunk;
}

void unmap_chunk(std::vector<std::span<uint32_t>> mapped_chunk) {
  for (auto mapped_segment : mapped_chunk){
    Assert((munmap(mapped_segment.data(), mapped_segment.size_bytes()) == 0), "Blub");
  }
}

int main() {
  std::cout << "Playground started." << std::endl;

  //const char* filename = "flattened_vector.txt";
  const auto COLUMN_COUNT = uint32_t{23};
  const auto ROW_COUNT = uint32_t{65000};
  auto chunk = create_chunk(ROW_COUNT, COLUMN_COUNT);
  const auto chunk_name = "test_chunk";

  // TODO: Make file removal before writing prettier.
  std::remove("test_chunk.bin");
  write_chunk(chunk, chunk_name);

  std::cout << "Finished writing." << std::endl;

  std::cout << "Start reading." << std::endl;
  const auto mapped_chunk = map_chunk(chunk_name, COLUMN_COUNT, ROW_COUNT);

  // calculate sum of column 17
  std::cout << "Sum of column 17 of created chunk: "
            << std::accumulate(chunk[16]->begin(), chunk[16]->end(), uint64_t{0}) << std::endl;
  std::cout << "Sum of column 17 of mapped chunk: "
            << std::accumulate(mapped_chunk[16].begin(), mapped_chunk[16].end(), uint64_t{0}) << std::endl;

  // print row 17
  std::cout << "Row 17 of created chunk: ";
  for (auto column_index = size_t{0}; column_index < COLUMN_COUNT; ++column_index) {
    std::cout << chunk[column_index]->at(16) << " ";
  }
  std::cout << std::endl;

  std::cout << "Row 17 of mapped chunk: ";
  for (auto column_index = size_t{0}; column_index < COLUMN_COUNT; ++column_index) {
    std::cout << mapped_chunk[column_index][16] << " ";
  }

  unmap_chunk(mapped_chunk);

  return 0;
}
