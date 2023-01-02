#include "micro_benchmark_utils.hpp"
#include "utils/assert.hpp"

#include <aio.h>
#include <stddef.h>
#include <unistd.h>
#include <fstream>

#include <algorithm>
#include <cstring>
#include <random>

namespace hyrise {

void micro_benchmark_clear_cache() {
  constexpr auto ITEM_COUNT = 500 * 1000 * 1000;
  auto clear = std::vector<int>(ITEM_COUNT, 42);
  for (auto index = size_t{0}; index < ITEM_COUNT; ++index) {
    clear[index] += 1;
  }
}

void micro_benchmark_clear_disk_cache() {
  // TODO(phoenix): better documentation of which caches we are clearing
  sync();
  std::ofstream ofs("/proc/sys/vm/drop_caches");
  ofs << "3" << std::endl;
}

void aio_error_handling(aiocb* aiocb, uint32_t expected_bytes) {
  const auto err = aio_error(aiocb);
  const auto ret = aio_return(aiocb);

  Assert(err == 0, "Error at aio_error(): " + std::strerror(errno));

  Assert(ret == static_cast<int32_t>(expected_bytes),
         "Error at aio_return(). Got: " + std::to_string(ret) + " Expected: " + std::to_string(expected_bytes) + ".");
}

/**
 * Generates a vector containing random indexes between 0 and number.
*/
std::vector<uint32_t> generate_random_indexes(uint32_t number) {
  std::vector<uint32_t> sequence(number);
  std::iota(std::begin(sequence), std::end(sequence), 0);
  auto rng = std::default_random_engine{};
  std::shuffle(std::begin(sequence), std::end(sequence), rng);
  return sequence;
}

std::vector<uint32_t> generate_random_indexes_page_fault(uint32_t number) {
  std::vector<uint32_t> sequence(number);
  // number of elements 2500608
  // page size: 4096 byte
  // size of uint32_t = 4 byte
  // elements per page: 4096 / 4 = 1024
  auto elements_per_page = 4096 / sizeof(uint32_t);

  for (auto index = size_t{0}; index < number; ++index) {
    auto result = index * elements_per_page;
    auto int_div_result = static_cast<uint32_t>(result/number);
    auto value = (result) % number + int_div_result;
    sequence[index] = value;
  }

  return sequence;
}

std::vector<uint32_t> generate_random_positive_numbers(uint32_t size) {
  auto numbers = std::vector<uint32_t>(size);
  for (auto index = size_t{0}; index < size; ++index) {
    numbers[index] = std::rand() % UINT32_MAX;
  }

  return numbers;
}

std::string fail_and_close_file(int32_t fd, std::string message, int error_num) {
  close(fd);
  return message + std::strerror(error_num);
}
}  // namespace hyrise
