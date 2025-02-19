#include "micro_benchmark_utils.hpp"
#include "micro_benchmark_basic_fixture.hpp"
#include "utils/assert.hpp"

#include <aio.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
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
#ifdef __APPLE__
  auto return_val = system("purge");
  (void)return_val;
#else
  auto return_val = system("echo 3 > /proc/sys/vm/drop_caches");
  (void)return_val;
#endif
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
std::vector<uint64_t> generate_random_indexes(uint64_t size) {
  std::vector<uint64_t> sequence(size);
  std::iota(std::begin(sequence), std::end(sequence), 0);
  auto rng = std::default_random_engine{};
  std::shuffle(std::begin(sequence), std::end(sequence), rng);
  return sequence;
}

std::vector<uint32_t> generate_random_positive_numbers(uint64_t size) {
  auto numbers = std::vector<uint32_t>(size);
  for (auto index = uint64_t{0}; index < size; ++index) {
    numbers[index] = std::rand() % UINT32_MAX;
  }

  return numbers;
}

std::string close_file_and_return_error_message(int32_t fd, std::string message, const int error_num) {
  close(fd);
  return message + std::strerror(error_num);
}

std::string close_files_and_return_error_message(std::vector<int32_t> filedescriptors, std::string message,
                                                 const int error_num) {
  for (auto index = size_t{0}; index < filedescriptors.size(); ++index) {
    close(filedescriptors[index]);
  }
  return message + std::strerror(error_num);
}

// Arguments are file size in MB
void CustomArguments(benchmark::internal::Benchmark* benchmark) {
  const std::vector<uint32_t> parameters = {10000, 100000};
  const std::vector<uint8_t> thread_counts = {1, 2, 4, 8, 16, 24, 32, 40, 48, 56, 64};

  for (auto param_index = size_t{0}; param_index < parameters.size(); ++param_index)
    for (auto thread_index = size_t{0}; thread_index < thread_counts.size(); ++thread_index)
      benchmark->Args({parameters[param_index], thread_counts[thread_index]});
}

}  // namespace hyrise
