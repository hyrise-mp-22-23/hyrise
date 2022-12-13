
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <numeric>
#include <span>
#include <sys/mman.h>
#include <vector>
#include "types.hpp"

#include <unistd.h>


using namespace hyrise;  // NOLINT

#define chunk_prototype std::vector<std::shared_ptr<std::vector<uint32_t>>>

std::string fail_and_close_file(int32_t fd, std::string message, int error_num) {
  close(fd);
  return message + std::strerror(error_num);
}

chunk_prototype create_chunk(const uint32_t row_count, const uint32_t column_count) {
  auto chunk = chunk_prototype{};
  const auto VALUE_AMOUNT = column_count * row_count;

  std::cout << "We create a chunk with " << column_count << " columns, " << row_count << " rows and thus "
            << VALUE_AMOUNT << " values." << std::endl;
  chunk.reserve(column_count);

  // create columns
  for (auto index = uint32_t{0}; index < column_count; ++index) {
    auto new_column = std::vector<uint32_t>{};
    new_column.reserve(row_count);
    chunk.push_back(std::make_shared<std::vector<uint32_t>>(new_column));
  }

  // create rows / insert values
  for (auto index = uint32_t{0}; index < VALUE_AMOUNT; ++index) {
    const auto column_index = index % column_count;
    auto column = chunk.at(column_index);
    column->emplace_back(index);
  }

  return chunk;
}

void write_segment(const std::shared_ptr<std::vector<uint32_t>> segment, const std::string& filename){
  std::ofstream column_file;
  column_file.open(filename, std::ios::out | std::ios::binary);
  column_file.write(reinterpret_cast<char*>(std::data(*segment)), segment->size() * sizeof(uint32_t));
  column_file.close();
}

void write_chunk(const chunk_prototype& chunk){
  const auto file_prefix = "test_chunk_segment";
  const auto file_extension = ".bin";
  const auto column_count = chunk.size();
  for (auto index = size_t{0}; index < column_count; ++index){
    const auto filename = file_prefix + std::to_string(index) + file_extension;
    write_segment(chunk[index], filename);
  }
}

std::vector<uint32_t*> map_chunk(const std::string& chunk_name, const uint32_t column_count, const uint32_t chunk_size){
  auto mapped_chunk = std::vector<uint32_t*>();
  for (auto column_index = size_t{0}; column_index < column_count; ++column_index){
    auto fd = int32_t{};
    const auto column_filename = chunk_name + "_segment" + std::to_string(column_index) + ".bin";
    Assert((fd = open(column_filename.c_str(), O_RDONLY)) >= 0, fail_and_close_file(fd, "Open error: ", errno));

    const auto offset = off_t{0};
    const auto chunk_bytes = chunk_size * sizeof(uint32_t);

    auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, chunk_bytes, PROT_READ, MAP_PRIVATE, fd, offset));
    Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));

    madvise(map, chunk_bytes, MADV_SEQUENTIAL);
    mapped_chunk.emplace_back(map);
  }
  return mapped_chunk;
}

int main() {
  std::cout << "Playground started." << std::endl;

  //const char* filename = "flattened_vector.txt";
  const auto COLUMN_COUNT = uint32_t{23};
  const auto ROW_COUNT = uint32_t{65000};
  auto chunk = create_chunk(ROW_COUNT, COLUMN_COUNT);
  write_chunk(chunk);

  std::cout << "Finished writing." << std::endl;

  std::cout << "Start reading." << std::endl;
  const auto chunk_name = "test_chunk";
  const auto mapped_chunk = map_chunk(chunk_name, COLUMN_COUNT, ROW_COUNT);

  // calculate sum of column 17
  auto sum_column_17 = uint64_t{0};
  const auto column_17 = mapped_chunk[16];
  for (auto index = size_t{0}; index < ROW_COUNT; ++index) {
    sum_column_17 += column_17[index];
  }

  std::cout << "Sum of column 17 of created chunk: " << std::accumulate(chunk[16]->begin(), chunk[16]->end(), uint64_t{0}) << std::endl;
  std::cout << "Sum of column 17 of mapped chunk: " << sum_column_17 << std::endl;

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
  std::cout << std::endl;

  return 0;
}
