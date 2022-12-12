
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

void print_data(chunk_type data) {
  const ssize_t ROW_COUNT = data.size();
  const ssize_t COLUMN_COUNT = data.at(0)->size();
  std::cout << "Data:" << std::endl;
  for (auto column_index = ssize_t{0}; column_index < COLUMN_COUNT; column_index++) {
    for (auto row_index = ssize_t{0}; row_index < ROW_COUNT; row_index++) {
      std::cout << data.at(row_index)->at(column_index) << " ";
    }
    std::cout << std::endl;
  }
}

void print_vector(std::vector<uint32_t> values) {
  std::cout << "Flattend vector:" << std::endl;
  for (auto value : values) {
    std::cout << value << ' ';
  }
  std::cout << std::endl;
}

std::vector<uint32_t> flatten(chunk_type const& chunk) {
  const ssize_t ROW_COUNT = chunk.size();
  const ssize_t COLUMN_COUNT = chunk.at(0)->size();

  auto flattened = std::vector<uint32_t>();
  flattened.reserve(ROW_COUNT * COLUMN_COUNT);

  for (auto column_index = ssize_t{0}; column_index < COLUMN_COUNT; column_index++) {
    for (auto row_index = ssize_t{0}; row_index < ROW_COUNT; row_index++) {
      flattened.push_back(chunk.at(row_index)->at(column_index));
    }
  }
  return flattened;
}

chunk_type create_chunk(const uint32_t row_count, const uint32_t column_count) {
  Assert(row_count % column_count == 0, "Row count is not a multiple of column count!");
  auto chunk = chunk_type{};
  const auto VALUE_AMOUNT = column_count * row_count;

  std::cout << "We want to create one chunk with " << column_count << " columns, " << row_count << " rows and thus "
            << VALUE_AMOUNT << " values." << std::endl;
  chunk.reserve(column_count);

  // create columns
  for (auto index = ssize_t{0}; index < column_count; ++index) {
    auto new_column = std::vector<uint32_t>{};
    new_column.reserve(row_count);
    chunk.push_back(std::make_shared<std::vector<uint32_t>>(new_column));
  }

  std::cout << chunk.size() << " columns created." << std::endl;

  // create rows / insert values
  for (auto index = ssize_t{0}; index < VALUE_AMOUNT; ++index) {
    auto column_index = index % column_count;
    auto column = chunk.at(column_index);
    column->push_back(index);
  }

  std::cout << "Values populated." << std::endl;
  return chunk;
}

void write_data_to_file(std::vector<uint32_t> flattened_chunk) {
  const auto NUMBER_OF_BYTES = static_cast<ssize_t>(flattened_chunk.size() * sizeof(uint32_t));
  const char* filename = "flattened_vector.txt";
  auto fd = int32_t{};
  Assert(((fd = creat(filename, O_WRONLY)) >= 1), fail_and_close_file(fd, "Create error: ", errno));
  chmod(filename, S_IRWXU);  // enables owner to rwx file
  Assert((write(fd, std::data(flattened_chunk), NUMBER_OF_BYTES) == NUMBER_OF_BYTES),
         fail_and_close_file(fd, "Write error: ", errno));
  close(fd);
}

std::vector<uint32_t> read_data_from_file(){
  auto read_data = std::vector<u_int32_t>{};
  //TODO(@everyone) Reading steps + evaluation
  return read_data;
}

int main() {
  std::cout << "Playground started." << std::endl;
  const auto COLUMN_COUNT = uint32_t{5};
  const auto ROW_COUNT = uint32_t{10};
  auto chunk = create_chunk(ROW_COUNT, COLUMN_COUNT);
  print_data(chunk);
  auto flattened_chunk = flatten(chunk);
  print_vector(flattened_chunk);
  write_data_to_file(flattened_chunk);
  return 0;
}
