
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include "types.hpp"

using namespace hyrise;  // NOLINT

#define chunk_type std::vector<std::shared_ptr<std::vector<int32_t>>>

std::string fail_and_close_file(int32_t fd, std::string message, int error_num) {
  close(fd);
  return message + std::strerror(error_num);
}

void printData(chunk_type data) {
  const ssize_t ROW_COUNT = data.size();
  const ssize_t COLUMN_COUNT = data.at(0)->size();
  std::cout << "Data:" << std::endl;
  for (ssize_t i = 0; i < COLUMN_COUNT; i++) {
    for (ssize_t j = 0; j < ROW_COUNT; j++) {
      std::cout << data.at(j)->at(i) << " ";
    }
    std::cout << std::endl;
  }
}

void printVector(std::vector<int32_t> vector) {
  std::cout << "Vector:" << std::endl;
  for (int32_t i : vector) {
    std::cout << i << ' ';
  }
  std::cout << std::endl;
}

std::vector<int32_t> flatten(chunk_type const& chunk) {
  const ssize_t ROW_COUNT = chunk.size();
  const ssize_t COLUMN_COUNT = chunk.at(0)->size();

  std::vector<int32_t> flattened;
  flattened.reserve(ROW_COUNT * COLUMN_COUNT);

  for (ssize_t i = 0; i < COLUMN_COUNT; i++) {
    for (ssize_t j = 0; j < ROW_COUNT; j++) {
      flattened.push_back(chunk.at(j)->at(i));
    }
  }
  return flattened;
}

chunk_type createChunk(const uint32_t row_count, const uint32_t column_count) {
  auto chunk = chunk_type{};
  const auto VALUE_AMOUNT = column_count * row_count;

  std::cout << "We want to create one chunk with " << column_count << " columns, " << row_count << " rows and thus "
            << VALUE_AMOUNT << " values." << std::endl;
  chunk.reserve(column_count);

  // create columns
  for (uint32_t i = 0; i < column_count; ++i) {
    auto new_column = std::vector<int32_t>{};
    new_column.reserve(row_count);
    chunk.push_back(std::make_shared<std::vector<int32_t>>(new_column));
  }

  std::cout << chunk.size() << " columns created." << std::endl;

  // create rows / insert values
  for (uint32_t i = 0; i < VALUE_AMOUNT; ++i) {
    auto column_index = i % column_count;
    auto column = chunk.at(column_index);
    column->push_back(i*column_index);
  }

  std::cout << "Values populated." << std::endl;
  return chunk;
}

void writeDataToFile(std::vector<int32_t> flattened_chunk, const char* filename) {
  const ssize_t NUMBER_OF_BYTES = flattened_chunk.size() * sizeof(int32_t);
  auto fd = int32_t{};
  Assert(((fd = creat(filename, O_WRONLY)) >= 1), fail_and_close_file(fd, "Create error: ", errno));
  chmod(filename, S_IRWXU);  // enables owner to rwx file
  Assert((write(fd, std::data(flattened_chunk), NUMBER_OF_BYTES) == NUMBER_OF_BYTES),
         fail_and_close_file(fd, "Write error: ", errno));
  close(fd);
}

std::vector<int32_t> readDataFromFile(size_t number_of_bytes, const char* filename) {
  auto read_data = std::vector<int32_t>{};
  const auto NUMBER_OF_ITEMS = number_of_bytes / sizeof(int32_t);
  read_data.reserve(NUMBER_OF_ITEMS);
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  // Getting the mapping to memory.
  const auto OFFSET = off_t{0};
  auto* map = reinterpret_cast<u_int32_t*>(mmap(NULL, number_of_bytes, PROT_READ, MAP_PRIVATE, fd, OFFSET));
  Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));

  madvise(map, number_of_bytes, MADV_SEQUENTIAL);
  for (auto index = size_t{0}; index < NUMBER_OF_ITEMS; ++index) {
    read_data.push_back(map[index]);
  }

  // Remove memory mapping after job is done.
  Assert((munmap(map, number_of_bytes) == 0), fail_and_close_file(fd, "Unmapping failed: ", errno));
  return read_data;
}

chunk_type recreateChunk(const uint32_t row_count, const uint32_t column_count, std::vector<int32_t> data_vector) {
  Assert(row_count % column_count == 0, "Row count is not a multiple of column count!");
  auto chunk = chunk_type{};
  const auto VALUE_AMOUNT = data_vector.size();

  std::cout << "We want to recreate one chunk with " << column_count << " columns, " << row_count << " rows and thus "
            << VALUE_AMOUNT << " values." << std::endl;
  chunk.reserve(column_count);

  // create columns
  for (uint32_t i = 0; i < column_count; ++i) {
    auto new_column = std::vector<int32_t>{};
    new_column.reserve(row_count);
    chunk.push_back(std::make_shared<std::vector<int32_t>>(new_column));
  }

  std::cout << chunk.size() << " columns created." << std::endl;

  // create rows / insert values
  for (size_t i = 0; i < VALUE_AMOUNT; ++i) {
    auto column_index = i % column_count;
    auto column = chunk.at(column_index);
    column->push_back(data_vector.at(i));
  }

  std::cout << "Values loaded." << std::endl;
  return chunk;
}

chunk_type readDataFromFileAsChunk(const uint32_t column_count, size_t number_of_bytes,
                                   const char* filename) {
  const auto NUMBER_OF_ITEMS = number_of_bytes / sizeof(int32_t);
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  // Getting the mapping to memory.
  const auto OFFSET = off_t{0};
  auto* map = reinterpret_cast<u_int32_t*>(mmap(NULL, number_of_bytes, PROT_READ, MAP_PRIVATE, fd, OFFSET));
  Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));

  madvise(map, number_of_bytes, MADV_SEQUENTIAL);

  auto chunk = chunk_type{};
  chunk.reserve(column_count);

  for (size_t index = 0; index < NUMBER_OF_ITEMS; ++index) {
    auto column_index = index % column_count;
    if (column_index < chunk.size()) {
      auto column = chunk.at(column_index);
      column->push_back(map[index]);
    } else {
      auto new_column = std::vector<int32_t>{};
      new_column.push_back(map[index]);
      chunk.push_back(std::make_shared<std::vector<int32_t>>(new_column));
    }
  }

  // Remove memory mapping after job is done.
  Assert((munmap(map, number_of_bytes) == 0), fail_and_close_file(fd, "Unmapping failed: ", errno));
  return chunk;
}

void sanityCheck(chunk_type original_chunk, chunk_type read_chunk) {
  const size_t ROW_COUNT = original_chunk.size();
  const size_t COLUMN_COUNT = original_chunk.at(0)->size();

  Assert(ROW_COUNT == read_chunk.size(), "Amount of rows doesn't match!");
  Assert(COLUMN_COUNT == read_chunk.at(0)->size(), "Amount of columns doesn't match!");

  for (size_t i = 0; i < COLUMN_COUNT; i++) {
    for (size_t j = 0; j < ROW_COUNT; j++) {
      auto original_value = original_chunk.at(j)->at(i);
      auto read_value = read_chunk.at(j)->at(i);
      Assert(original_value == read_value, "Values are not the same!");
    }
  }
}

int main() {
  std::cout << "Playground started." << std::endl;
  const char* filename = "flattened_vector.txt";
  const auto COLUMN_COUNT = uint32_t{23};
  const auto ROW_COUNT = uint32_t{65000};
  auto chunk = createChunk(ROW_COUNT, COLUMN_COUNT);
  auto flattened_chunk = flatten(chunk);
  const ssize_t NUMBER_OF_BYTES = flattened_chunk.size() * sizeof(int32_t);
  writeDataToFile(flattened_chunk, filename);

  std::cout << "Finished writing." << std::endl;
  std::cout << "Start reading." << std::endl;

  /*
  auto read_vector = readDataFromFile(NUMBER_OF_BYTES, filename);
  printVector(read_vector);
  auto read_chunk = recreateChunk(ROW_COUNT, COLUMN_COUNT, read_vector);
   or */
  auto read_chunk = readDataFromFileAsChunk(COLUMN_COUNT, NUMBER_OF_BYTES, filename);

  sanityCheck(chunk, read_chunk);
  return 0;
}
