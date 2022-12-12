
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <numeric>
#include <vector>
#include "types.hpp"

using namespace hyrise;  // NOLINT

#define chunk_type std::vector<std::shared_ptr<std::vector<int32_t>>>

std::string fail_and_close_file(int32_t fd, std::string message, int error_num) {
  close(fd);
  return message + std::strerror(error_num);
}

void print_data(chunk_type data) {
  const ssize_t ROW_COUNT = data.size();
  const ssize_t COLUMN_COUNT = data.at(0)->size();
  std::cout << "Data:" << std::endl;
  for ( auto column_index = ssize_t{0}; column_index < COLUMN_COUNT; column_index++) {
    for (auto row_index = ssize_t{0}; row_index < ROW_COUNT; row_index++) {
      std::cout << data.at(row_index)->at(column_index) << " ";
    }
    std::cout << std::endl;
  }
}

void printVector(std::vector<int32_t> values) {
  std::cout << "Vector:" << std::endl;
  for (auto value : values) {
    std::cout << value << ' ';
  }
  std::cout << std::endl;
}

std::vector<int32_t> flatten(chunk_type const& chunk) {
  const ssize_t ROW_COUNT = chunk.size();
  const ssize_t COLUMN_COUNT = chunk.at(0)->size();

  auto flattened = std::vector<int32_t>();
  flattened.reserve(ROW_COUNT * COLUMN_COUNT);

  for (auto column_index = ssize_t{0}; column_index < COLUMN_COUNT; column_index++) {
    for (auto row_index = ssize_t{0}; row_index < ROW_COUNT; row_index++) {
      flattened.push_back(chunk.at(row_index)->at(column_index));
    }
  }
  return flattened;
}

chunk_type create_chunk(const uint32_t row_count, const uint32_t column_count) {
  auto chunk = chunk_type{};
  const auto VALUE_AMOUNT = column_count * row_count;

  std::cout << "We want to create one chunk with " << column_count << " columns, " << row_count << " rows and thus "
            << VALUE_AMOUNT << " values." << std::endl;
  chunk.reserve(column_count);

  // create columns
  for (auto index = ssize_t{0}; index < column_count; ++index) {
    auto new_column = std::vector<int32_t>{};
    new_column.reserve(row_count);
    chunk.push_back(std::make_shared<std::vector<int32_t>>(new_column));
  }

  std::cout << chunk.size() << " columns created." << std::endl;

  // create rows / insert values
  for (auto index = ssize_t{0}; index < VALUE_AMOUNT; ++index) {
    auto column_index = index % column_count;
    auto column = chunk.at(column_index);
    column->push_back(index * column_index);
  }

  std::cout << "Values populated." << std::endl;
  return chunk;
}

void write_data_to_file(std::vector<int32_t> flattened_chunk, const char* filename) {
  const auto NUMBER_OF_BYTES = static_cast<ssize_t>(flattened_chunk.size() * sizeof(int32_t));
  auto fd = int32_t{};
  Assert(((fd = creat(filename, O_WRONLY)) >= 1), fail_and_close_file(fd, "Create error: ", errno));
  chmod(filename, S_IRWXU);  // enables owner to rwx file
  Assert((write(fd, std::data(flattened_chunk), NUMBER_OF_BYTES) == NUMBER_OF_BYTES),
         fail_and_close_file(fd, "Write error: ", errno));
  close(fd);
}

std::vector<int32_t> read_data_from_file(size_t number_of_bytes, const char* filename) {
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

chunk_type recreate_chunk(const uint32_t row_count, const uint32_t column_count, std::vector<int32_t> data_vector) {
  Assert(row_count % column_count == 0, "Row count is not a multiple of column count!");
  auto chunk = chunk_type{};
  const auto VALUE_AMOUNT = data_vector.size();

  std::cout << "We want to recreate one chunk with " << column_count << " columns, " << row_count << " rows and thus "
            << VALUE_AMOUNT << " values." << std::endl;
  chunk.reserve(column_count);

  // create columns
  for (auto index = size_t{0}; index < column_count; ++index) {
    auto new_column = std::vector<int32_t>{};
    new_column.reserve(row_count);
    chunk.push_back(std::make_shared<std::vector<int32_t>>(new_column));
  }

  std::cout << chunk.size() << " columns created." << std::endl;

  // create rows / insert values
  for (auto index = size_t{0}; index < VALUE_AMOUNT; ++index) {
    auto column_index = index % column_count;
    auto column = chunk.at(column_index);
    column->push_back(data_vector.at(index));
  }

  std::cout << "Values loaded." << std::endl;
  return chunk;
}

chunk_type read_data_from_file_as_chunk(const uint32_t column_count, size_t number_of_bytes, const char* filename) {
  const auto NUMBER_OF_ITEMS = number_of_bytes / sizeof(int32_t);
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  // Getting the mapping to memory.
  const auto OFFSET = off_t{0};
  auto* map = reinterpret_cast<int32_t*>(mmap(NULL, number_of_bytes, PROT_READ, MAP_PRIVATE, fd, OFFSET));
  Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));

  madvise(map, number_of_bytes, MADV_SEQUENTIAL);

  auto chunk = chunk_type{};
  chunk.reserve(column_count);

  for (auto index = size_t{0}; index < NUMBER_OF_ITEMS; ++index) {
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

void sanity_check(chunk_type original_chunk, chunk_type read_chunk) {
  const auto ROW_COUNT = original_chunk.size();
  const auto COLUMN_COUNT = original_chunk.at(0)->size();

  Assert(ROW_COUNT == read_chunk.size(), "Amount of rows doesn't match!");
  Assert(COLUMN_COUNT == read_chunk.at(0)->size(), "Amount of columns doesn't match!");

  for (auto column_index = size_t{0}; column_index < COLUMN_COUNT; column_index++) {
    for (auto row_index = size_t{0}; row_index < ROW_COUNT; row_index++) {
      auto original_value = original_chunk.at(row_index)->at(column_index);
      auto read_value = read_chunk.at(row_index)->at(column_index);
      Assert(original_value == read_value, "Values are not the same!");
    }
  }
}

int calculate_sum_across_column(chunk_type chunk, u_int32_t col_index) {
  auto column = chunk.at(col_index);
  auto sum = std::accumulate(column->begin(), column->end(), uint64_t{0});
  return sum;
}

void print_row(chunk_type chunk, u_int32_t row_index) {
  const auto COLUMN_COUNT = static_cast<ssize_t>(chunk.size());
  std::cout << "Data of row: " << row_index << std::endl;
  for (auto index = ssize_t{0}; index < COLUMN_COUNT; ++index) {
    std::cout << chunk.at(index)->at(row_index) << " ";
  }
  std::cout << std::endl;
}

int main() {
  std::cout << "Playground started." << std::endl;
  const char* filename = "flattened_vector.txt";
  const auto COLUMN_COUNT = uint32_t{23};
  const auto ROW_COUNT = uint32_t{65000};
  auto chunk = create_chunk(ROW_COUNT, COLUMN_COUNT);
  auto flattened_chunk = flatten(chunk);
  const ssize_t NUMBER_OF_BYTES = flattened_chunk.size() * sizeof(int32_t);
  write_data_to_file(flattened_chunk, filename);

  std::cout << "Finished writing." << std::endl;
  std::cout << "Start reading." << std::endl;

  /*
  auto read_vector = readDataFromFile(NUMBER_OF_BYTES, filename);
  printVector(read_vector);
  auto read_chunk = recreateChunk(ROW_COUNT, COLUMN_COUNT, read_vector);
   or */
  auto read_chunk = read_data_from_file_as_chunk(COLUMN_COUNT, NUMBER_OF_BYTES, filename);
  sanity_check(chunk, read_chunk);
  auto sum = calculate_sum_across_column(read_chunk, 17);
  std::cout << "Sum of column " << 17 << ": " << sum << std::endl;
  print_row(chunk, 17);
  return 0;
}
