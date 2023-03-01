#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <numeric>
#include <span>

#include <cstddef>
#include <map>
#include <vector>

#include "micro_benchmark_basic_fixture.hpp"

namespace hyrise {

class FileIOMicroReadBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void create_random_indexes_if_needed(size_t size_parameter, uint64_t number_of_elements) {
    if (random_indexes.empty() || last_size_parameter != size_parameter) {
      random_indexes = generate_random_indexes(number_of_elements);
      last_size_parameter = size_parameter;
    }
  }

  void SetUp(::benchmark::State& state) override {
    const auto size_parameter = state.range(0);

    NUMBER_OF_BYTES = _align_to_pagesize(size_parameter);
    NUMBER_OF_ELEMENTS = NUMBER_OF_BYTES / uint32_t_size;

    numbers = generate_random_positive_numbers(NUMBER_OF_ELEMENTS);
    control_sum = std::accumulate(numbers.begin(), numbers.end(), uint64_t{0});

    filename = "benchmark_data_" + std::to_string(size_parameter) + ".bin";
    std::ofstream file(filename, std::ios::binary);
    file.write(reinterpret_cast<const char*>(numbers.data()), numbers.size() * sizeof(uint32_t));
    chmod(filename.c_str(), S_IRWXU);  // enables owner to rwx file
    file.close();
  }

  void TearDown(::benchmark::State& /*state*/) override {
    Assert(std::remove(filename.c_str()) == 0, "Remove error: " + std::strerror(errno));
  }

 protected:
  const ssize_t uint32_t_size = ssize_t{sizeof(uint32_t)};
  // read / write accept at most up to = 2,147,479,552 bytes
  const uint64_t MAX_NUMBER_OF_ELEMENTS = uint64_t{536'869'888};
  std::string filename;
  uint64_t control_sum = uint64_t{0};
  uint64_t NUMBER_OF_BYTES = uint64_t{0};
  uint64_t NUMBER_OF_ELEMENTS = uint64_t{0};
  size_t last_size_parameter;
  std::vector<uint32_t> numbers = std::vector<uint32_t>{};
  std::vector<uint64_t> random_indexes = std::vector<uint64_t>{};
  void read_non_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void read_non_atomic_single_threaded(benchmark::State& state);
  void read_non_atomic_random_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void read_non_atomic_random_single_threaded(benchmark::State& state);
  void pread_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void pread_atomic_single_threaded(benchmark::State& state);
  void pread_atomic_random_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void pread_atomic_random_single_threaded(benchmark::State& state);
#ifdef __linux__
  void libaio_sequential_read_single_threaded(benchmark::State& state);
  void libaio_sequential_read_multi_threaded(benchmark::State& state, uint16_t aio_request_count);
  void libaio_random_read(benchmark::State& state, uint16_t aio_request_count);
#endif
  void memory_mapped_read_single_threaded(benchmark::State& state, const int mapping_type, const int map_mode_flag,
                                          const int access_order);
#ifdef __linux__
  void memory_mapped_read_user_space(benchmark::State& state, const uint16_t thread_count, const int access_order);
#endif
  void memory_mapped_read_multi_threaded(benchmark::State& state, const int mapping_type, const int map_mode_flag,
                                         const uint16_t thread_count, const int access_order);

  // enums for mmap benchmarks
  enum MAPPING_TYPE { MMAP, UMAP };

  enum DATA_ACCESS_TYPES { SEQUENTIAL, RANDOM };

  enum MAP_ACCESS_TYPES { SHARED = MAP_SHARED, PRIVATE = MAP_PRIVATE };
};
}  // namespace hyrise
