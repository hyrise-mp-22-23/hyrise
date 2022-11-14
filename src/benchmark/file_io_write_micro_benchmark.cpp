#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>

#include "micro_benchmark_basic_fixture.hpp"


namespace hyrise {

const int32_t MB = 1000000;

class FileIOWriteMicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    // TODO(anyone): Make setup/teardown global per file size to improve benchmark speed
    ssize_t BUFFER_SIZE_MB = state.range(0);
    // each int32_t contains four bytes
    int32_t vector_element_count = (BUFFER_SIZE_MB * MB) / 4;
    data_to_write = std::vector<int32_t>(vector_element_count, 42);

    if (creat("file.txt", O_WRONLY) < 1) {
      std::cout << "create error" << std::endl;
    }
    chmod("file.txt", S_IRWXU);  // enables owner to rwx file
  }

  void TearDown(::benchmark::State& /*state*/) override {
    // TODO(anyone): Error handling
    std::remove("file.txt");
  }

 protected:
  std::vector<int32_t> data_to_write;
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
  }
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE)(benchmark::State& state) {
  const int32_t NUMBER_OF_BYTES = state.range(0) * MB;

  int32_t fd;
  if ((fd = open("file.txt", O_RDWR | O_CREAT | O_TRUNC)) < 0) {
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
    MAP_PRIVATE:
      "Updates to the mapping are not visible to other processes 
      mapping the same file"
      "changes are not carried through to the underlying files"
    */
    auto map = mmap(NULL, NUMBER_OF_BYTES, PROT_WRITE, MAP_PRIVATE, fd, OFFSET);
    if (map == MAP_FAILED) {
      std::cout << "Mapping Failed. " << std::strerror(errno) << std::endl;
      continue;
    }

    // Due to mmap, only writing in memory is required.
    memset(map, *(std::data(data_to_write)), NUMBER_OF_BYTES);
    // After writing, sync changes to filesystem.
    if (msync(map, NUMBER_OF_BYTES, MS_SYNC) == -1) {
      std::cout << "Write error " << errno << std::endl;
    }

    // Remove memory mapping after job is done.
    if (munmap(map, NUMBER_OF_BYTES) != 0) {
      std::cout << "Unmapping failed." << std::endl;
    }
  }
}

BENCHMARK_DEFINE_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED)(benchmark::State& state) {
  const int32_t NUMBER_OF_BYTES = state.range(0) * MB;

  int32_t fd;
  if ((fd = open("file.txt", O_RDWR | O_CREAT | O_TRUNC)) < 0) {
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
    auto map = mmap(NULL, NUMBER_OF_BYTES, PROT_WRITE, MAP_SHARED, fd, OFFSET);
    if (map == MAP_FAILED) {
      std::cout << "Mapping Failed. " << std::strerror(errno) << std::endl;
      continue;
    }

    // Due to mmap, only writing in memory is required.
    memset(map, *(std::data(data_to_write)), NUMBER_OF_BYTES);
    // After writing, sync changes to filesystem.
    if (msync(map, NUMBER_OF_BYTES, MS_SYNC) == -1) {
      std::cout << "Write error " << errno << std::endl;
    }

    // Remove memory mapping after job is done.
    if (munmap(map, NUMBER_OF_BYTES) != 0) {
      std::cout << "Unmapping failed." << std::endl;
    }
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

// arguments are file size in MB
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, WRITE_NON_ATOMIC)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, PWRITE_ATOMIC)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOWriteMicroBenchmarkFixture, IN_MEMORY_WRITE)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace hyrise
