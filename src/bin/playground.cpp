
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "types.hpp"

using namespace hyrise;  // NOLINT

#define chunk_type std::vector<std::shared_ptr<std::vector<uint32_t>>>

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

void printVector(std::vector<uint32_t> vector) {
  std::cout << "Flattend vector:" << std::endl;
  for (uint32_t i : vector) {
    std::cout << i << ' ';
  }
  std::cout << std::endl;
}

std::vector<uint32_t> flatten(chunk_type const& chunk) {
  const ssize_t ROW_COUNT = chunk.size();
  const ssize_t COLUMN_COUNT = chunk.at(0)->size();

  std::vector<uint32_t> flattened;
  flattened.reserve(ROW_COUNT * COLUMN_COUNT);

  for (ssize_t i = 0; i < COLUMN_COUNT; i++) {
    for (ssize_t j = 0; j < ROW_COUNT; j++) {
      flattened.push_back(chunk.at(j)->at(i));
    }
  }
  return flattened;
}

chunk_type createChunk(const uint32_t row_count, const uint32_t column_count) {
  Assert(row_count % column_count == 0, "Row count is not a multiple of column count!");
  auto chunk = chunk_type{};
  const auto VALUE_AMOUNT = column_count * row_count;

  std::cout << "We want to create one chunk with " << column_count << " columns, " << row_count << " rows and thus "
            << VALUE_AMOUNT << " values." << std::endl;
  chunk.reserve(column_count);

  // create columns
  for (uint32_t i = 0; i < column_count; ++i) {
    auto new_column = std::vector<uint32_t>{};
    new_column.reserve(row_count);
    chunk.push_back(std::make_shared<std::vector<uint32_t>>(new_column));
  }

  std::cout << chunk.size() << " columns created." << std::endl;

  // create rows / insert values
  for (uint32_t i = 0; i < VALUE_AMOUNT; ++i) {
    auto column_index = i % column_count;
    auto column = chunk.at(column_index);
    column->push_back(i);
  }

  std::cout << "Values populated." << std::endl;
  return chunk;
}

void writeDataToFile(std::vector<uint32_t> flattened_chunk) {
  const ssize_t NUMBER_OF_BYTES = flattened_chunk.size() * sizeof(uint32_t);
  const char* filename = "flattened_vector.txt";
  auto fd = int32_t{};
  Assert(((fd = creat(filename, O_WRONLY)) >= 1), fail_and_close_file(fd, "Create error: ", errno));
  chmod(filename, S_IRWXU);  // enables owner to rwx file
  Assert((write(fd, std::data(flattened_chunk), NUMBER_OF_BYTES) == NUMBER_OF_BYTES),
         fail_and_close_file(fd, "Write error: ", errno));
  close(fd);
}

std::vector<uint32_t> readDataFromFile(){
  auto read_data = std::vector<u_int32_t>{};
  //TODO(@everyone) Reading steps + evaluation
  return read_data;
}

int main() {
  std::cout << "Playground started." << std::endl;
  const auto COLUMN_COUNT = uint32_t{5};
  const auto ROW_COUNT = uint32_t{10};
  auto chunk = createChunk(ROW_COUNT, COLUMN_COUNT);
  printData(chunk);
  auto flattened_chunk = flatten(chunk);
  printVector(flattened_chunk);
  writeDataToFile(flattened_chunk);
  return 0;
}
