#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <numeric>
#include "micro_benchmark_basic_fixture.hpp"

namespace hyrise {

const int32_t MB = 1000000;

class FileIOWriteMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  uint64_t control_sum = uint64_t{0};

  void SetUp(::benchmark::State& state) override {
    // TODO(phoeinx): Make setup/teardown global per file size to improve benchmark speed
    ssize_t BUFFER_SIZE_MB = state.range(0);
    // each int32_t contains four bytes
    int32_t vector_element_count = (BUFFER_SIZE_MB * MB) / 4;
    data_to_write = std::vector<int32_t>(vector_element_count, 42);
    control_sum = vector_element_count * uint64_t{42};

    if (creat("file.txt", O_WRONLY) < 1) {
      std::cout << "create error" << std::endl;
    }
    chmod("file.txt", S_IRWXU);  // enables owner to rwx file
  }

  void SanityCheck(int32_t NUMBER_OF_BYTES){
    int32_t fd;
    if ((fd = open("file.txt", O_RDONLY)) < 0) {
      std::cout << "open error " << errno << std::endl;
    }
    chmod("file.txt", S_IRWXU);  // enables owner to rwx file
    std::vector<uint32_t> read_data;
    read_data.resize(NUMBER_OF_BYTES / 4);

    // Getting the mapping to memory.
    off_t OFFSET = 0;

    int32_t* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_PRIVATE, fd, OFFSET));
    if (map == MAP_FAILED) {
      std::cout << "Mapping Failed. " << std::strerror(errno) << std::endl;
    }

    memcpy(std::data(read_data), map, NUMBER_OF_BYTES);
    auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    // Remove memory mapping after job is done.
    if (munmap(map, NUMBER_OF_BYTES) != 0) {
      std::cout << "Unmapping failed." << std::endl;
    }
  }

  void TearDown(::benchmark::State& /*state*/) override {
    // TODO(phoeinx): Error handling
    std::remove("file.txt");
  }

 protected:
  std::vector<int32_t> data_to_write;
  void mmap_write_benchmark(benchmark::State& state, const int flag, int data_access_mode, const int32_t file_size);
};

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, WRITE_NON_ATOMIC)(benchmark::State& state) {  // open file
  int32_t fd;
  if ((fd = open("file.txt", O_WRONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }
  const int32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    if (write(fd, std::data(data_to_write), NUMBER_OF_BYTES) != NUMBER_OF_BYTES) {
      std::cout << "write error " << errno << std::endl;
    }

    state.PauseTiming();
    SanityCheck(NUMBER_OF_BYTES);
    state.ResumeTiming();
  }
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, PWRITE_ATOMIC)(benchmark::State& state) {
  int32_t fd;
  if ((fd = open("file.txt", O_WRONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }
  const int32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    if (pwrite(fd, std::data(data_to_write), NUMBER_OF_BYTES, 0) != NUMBER_OF_BYTES) {
      std::cout << "write error " << errno << std::endl;
    }

    state.PauseTiming();
    SanityCheck(NUMBER_OF_BYTES);
    state.ResumeTiming();
  }
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE)(benchmark::State& state) {
  mmap_write_benchmark(state, MAP_PRIVATE, 0, state.range(0));
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)(benchmark::State& state) {
  mmap_write_benchmark(state, MAP_SHARED, 0, state.range(0));
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)(benchmark::State& state) {
  mmap_write_benchmark(state, MAP_SHARED, 1, state.range(0));
}

/**
 * Performs a benchmark run with the given parameters. 
 * 
 * @arguments:
 *      state: the benchmark::State object handed to the called benchmarking function.
 *      flag: The mmap flag (e.g., MAP_PRIVATE or MAP_SHARED).
 *      data_access_mode: The way the data is written.
 *                  (-1)  No data access
 *                  (0)   Sequential
 *                  (1)   Random
 *      file_size: Size argument of benchmark.
*/
void FileIOWriteMicroBenchmarkFixture::mmap_write_benchmark(benchmark::State& state, const int flag,
                                                            int data_access_mode, const int32_t file_size) {
  const int32_t NUMBER_OF_BYTES = file_size * MB;

  int32_t fd;
  if ((fd = open("file.txt", O_RDWR)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }

  // set output file size
  if (ftruncate(fd, NUMBER_OF_BYTES) < 0) {
    std::cout << "ftruncate error " << errno << std::endl;
  }

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    off_t OFFSET = 0;
    /*
    mmap man page: 
    MAP_SHARED:
      "Updates to the mapping are visible to other processes mapping 
      the same region"
      "changes are carried through to the underlying files"
    */
    auto map = reinterpret_cast<char*>(mmap(NULL, NUMBER_OF_BYTES, PROT_WRITE, flag, fd, OFFSET));
    if (map == MAP_FAILED) {
      std::cout << "Mapping Failed. " << std::strerror(errno) << std::endl;
      continue;
    }

    switch (data_access_mode) {
      case 0:
        memcpy(map, std::data(data_to_write), NUMBER_OF_BYTES);
        break;
      case 1:
        state.PauseTiming();
        // Generating random indexes should not play a role in the benchmark.
        std::vector<int> ind_access_order = generate_random_indexes(NUMBER_OF_BYTES);
        state.ResumeTiming();
        for (uint32_t idx = 0; idx < ind_access_order.size(); ++idx) {
          auto access_index = ind_access_order[idx];
          map[access_index] = access_index;
        }
        break;
    }

    // After writing, sync changes to filesystem.
    if (msync(map, NUMBER_OF_BYTES, MS_SYNC) == -1) {
      std::cout << "Write error " << errno << std::endl;
    }

    // Remove memory mapping after job is done.
    if (munmap(map, NUMBER_OF_BYTES) != 0) {
      std::cout << "Unmapping failed." << std::endl;
    }

    state.PauseTiming();
    SanityCheck(NUMBER_OF_BYTES);
    state.ResumeTiming();
  }
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, IN_MEMORY_WRITE)(benchmark::State& state) {  // open file
  const int32_t NUMBER_OF_BYTES = state.range(0) * MB;

  std::vector<uint64_t> contents(NUMBER_OF_BYTES / sizeof(uint64_t));
  for (auto index = size_t{0}; index < contents.size(); index++) {
    contents[index] = std::rand() % UINT16_MAX;
  }
  std::vector<uint64_t> copy_of_contents;

  for (auto _ : state) {
    copy_of_contents = contents;
    state.PauseTiming();
    Assert(std::equal(copy_of_contents.begin(), copy_of_contents.end(), contents.begin()),
           "Sanity check failed: Not the same result");
    Assert(&copy_of_contents != &contents, "Sanity check failed: Same reference");
    state.ResumeTiming();
  }
}

// Arguments are file size in MB
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, WRITE_NON_ATOMIC)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, PWRITE_ATOMIC)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, IN_MEMORY_WRITE)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace hyrise
