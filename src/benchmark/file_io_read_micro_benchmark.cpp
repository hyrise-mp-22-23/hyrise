#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iterator>
#include <numeric>
#include <random>
#include "micro_benchmark_basic_fixture.hpp"

namespace hyrise {

const auto MB = uint32_t{1'000'000};

class FileIOMicroReadBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  uint64_t control_sum = uint64_t{0};
  std::vector<uint32_t> numbers = std::vector<uint32_t>{};
  uint32_t vector_element_count;

  void SetUp(::benchmark::State& state) override {
    // TODO(everybody): Make setup/teardown global per file size to improve benchmark speed
    auto BUFFER_SIZE_MB = state.range(0);

    // each int32_t contains four bytes
    vector_element_count = (BUFFER_SIZE_MB * MB) / sizeof(uint32_t);
    numbers = generate_random_numbers(vector_element_count);
    control_sum = std::accumulate(numbers.begin(), numbers.end(), uint64_t{0});

    auto fd = int32_t{};
    Assert(((fd = creat(filename, O_WRONLY)) >= 1), fail_and_close_file(fd, "Create error: ", errno));
    chmod(filename, S_IRWXU);  // enables owner to rwx file
    Assert((write(fd, std::data(numbers), BUFFER_SIZE_MB * MB) == BUFFER_SIZE_MB * MB), fail_and_close_file(fd, "Write error: ", errno));

    close(fd);
  }

  void TearDown(::benchmark::State& /*state*/) override {
    // TODO(everybody): Error handling
    Assert(std::remove(filename) == 0, "Remove error: " + std::strerror(errno));
  }

 protected:
  const char* filename = "file.txt";  //const char* needed for C-System Calls
};

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_SEQUENTIAL)(benchmark::State& state) {  // open file
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  const auto NUMBER_OF_BYTES = uint32_t{static_cast<uint32_t>(state.range(0) * MB)};
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto read_data_size = NUMBER_OF_BYTES / uint32_t_size;
  const auto max_read_data_size = static_cast<size_t>(read_data_size);

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(read_data_size);

    state.ResumeTiming();

    // TODO Remove looping
    for (auto index = size_t{0}; index < max_read_data_size; ++index) {
      lseek(fd, uint32_t_size * index, SEEK_SET);
      Assert((read(fd, std::data(read_data) + index, uint32_t_size) == uint32_t_size), fail_and_close_file(fd, "Read error: ", errno));
    }

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_RANDOM)(benchmark::State& state) {  // open file
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  const auto NUMBER_OF_BYTES = uint32_t{static_cast<uint32_t>(state.range(0) * MB)};
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto read_data_size = NUMBER_OF_BYTES / uint32_t_size;
  const auto max_read_data_size = static_cast<size_t>(read_data_size);

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    const auto random_indices = generate_random_indexes(vector_element_count);
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(read_data_size);
    auto* read_data_start = std::data(read_data);

    state.ResumeTiming();

    lseek(fd, 0, SEEK_SET);
    // TODO Remove lseek loop
    for (auto index = size_t{0}; index < max_read_data_size; ++index) {
      lseek(fd, uint32_t_size * random_indices[index], SEEK_SET);
      // TODO do we need read data start?
      Assert((read(fd, read_data_start + index, uint32_t_size) == uint32_t_size), fail_and_close_file(fd, "Read error: ", errno));
    }

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_SEQUENTIAL)(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  const auto NUMBER_OF_BYTES = uint32_t{static_cast<uint32_t>(state.range(0) * MB)};
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto read_data_size = NUMBER_OF_BYTES / uint32_t_size;
  const auto max_read_data_size = static_cast<size_t>(read_data_size);

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(read_data_size);
    auto* read_data_start = std::data(read_data);

    state.ResumeTiming();

    // TODO remov loop
    for (auto index = size_t{0}; index < max_read_data_size; ++index) {
      // TODO remove read data start
      Assert((pread(fd, read_data_start + index, uint32_t_size, uint32_t_size * index) == uint32_t_size), fail_and_close_file(fd, "Read error: ", errno));
    }

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_RANDOM)(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  const uint32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    const auto random_indices = generate_random_indexes(vector_element_count);
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    auto* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_PRIVATE, fd, OFFSET));
    Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping failed: ", errno));

    madvise(map, NUMBER_OF_BYTES, MADV_RANDOM);

    auto sum = uint64_t{0};
    for (size_t index = 0; index < vector_element_count; ++index) {
      sum += map[random_indices[index]];
    }

    state.PauseTiming();
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();

    // Remove memory mapping after job is done.
    Assert((munmap(map, NUMBER_OF_BYTES) == 0), fail_and_close_file(fd, "Unmapping failed: ", errno));
  }
  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL)(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  const uint32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    auto* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_PRIVATE, fd, OFFSET));
    Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));

    madvise(map, NUMBER_OF_BYTES, MADV_SEQUENTIAL);

    auto sum = uint64_t{0};
    for (auto index = size_t{0}; index < vector_element_count; ++index) {
      sum += map[index];
    }

    state.PauseTiming();
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();

    // Remove memory mapping after job is done.
    Assert((munmap(map, NUMBER_OF_BYTES) == 0), fail_and_close_file(fd, "Unmapping failed: ", errno));
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  const uint32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    const auto random_indices = generate_random_indexes(vector_element_count);
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    auto* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_SHARED, fd, OFFSET));
    Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping failed: ", errno));

    madvise(map, NUMBER_OF_BYTES, MADV_RANDOM);

    auto sum = uint64_t{0};
    for (auto index = size_t{0}; index < vector_element_count; ++index) {
      sum += map[random_indices[index]];
    }

    state.PauseTiming();
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();

    // Remove memory mapping after job is done.
    Assert((munmap(map, NUMBER_OF_BYTES) == 0), fail_and_close_file(fd, "Unmapping failed: ", errno));
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  const uint32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    auto* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_SHARED, fd, OFFSET));
    Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping failed: ", errno));


    madvise(map, NUMBER_OF_BYTES, MADV_SEQUENTIAL);

    auto sum = uint64_t{0};
    for (size_t index = 0; index < vector_element_count; ++index) {
      sum += map[index];
    }

    state.PauseTiming();
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();

    // Remove memory mapping after job is done.
    Assert((munmap(map, NUMBER_OF_BYTES) == 0), fail_and_close_file(fd, "Unmapping failed: ", errno));
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_RANDOM)(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));


  const auto NUMBER_OF_BYTES = uint32_t{static_cast<uint32_t>(state.range(0) * MB)};
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto read_data_size = NUMBER_OF_BYTES / uint32_t_size;
  const auto max_read_data_size = static_cast<size_t>(read_data_size);

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    const auto random_indices = generate_random_indexes(vector_element_count);
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(read_data_size);
    auto* read_data_start = std::data(read_data);

    state.ResumeTiming();

    // TODO Remove looping
    for (auto index = size_t{0}; index < max_read_data_size; ++index) {
      Assert((pread(fd, read_data_start + index, uint32_t_size, uint32_t_size * random_indices[index]) == uint32_t_size), fail_and_close_file(fd, "Read error: ", errno));
    }

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_SEQUENTIAL)(benchmark::State& state) {  // open file
  const auto NUMBER_OF_BYTES = uint32_t{static_cast<uint32_t>(state.range(0) * MB)};
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto read_data_size = NUMBER_OF_BYTES / uint32_t_size;
  const auto max_read_data_size = static_cast<size_t>(read_data_size);

  for (auto _ : state) {
    state.PauseTiming();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(read_data_size);

    state.ResumeTiming();

    for (auto index = size_t{0}; index < max_read_data_size; ++index) {
      read_data[index] = numbers[index];
    }

    state.PauseTiming();
    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});

    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    Assert(&read_data != &numbers, "Sanity check failed: Same reference");

    state.ResumeTiming();
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_RANDOM)(benchmark::State& state) {  // open file
  const auto NUMBER_OF_BYTES = uint32_t{static_cast<uint32_t>(state.range(0) * MB)};
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto random_read_amount = static_cast<size_t>(NUMBER_OF_BYTES / uint32_t_size);

  for (auto _ : state) {
    state.PauseTiming();
    const auto random_indices = generate_random_indexes(vector_element_count);
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(random_read_amount);
    state.ResumeTiming();

    for (auto index = size_t{0}; index < random_read_amount; ++index) {
      read_data[index] = numbers[random_indices[index]];
    }

    state.PauseTiming();
    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});

    Assert(control_sum == static_cast<uint64_t>(sum), "Sanity check failed: Not the same result");
    Assert(&read_data[0] != &numbers[random_indices[0]], "Sanity check failed: Same reference");

    state.ResumeTiming();
  }
}

// Arguments are file size in MB
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_SEQUENTIAL)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_RANDOM)->Arg(10)->Arg(100)->Arg(1000);

BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_SEQUENTIAL)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_RANDOM)->Arg(10)->Arg(100)->Arg(1000);

BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_SEQUENTIAL)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_RANDOM)->Arg(10)->Arg(100)->Arg(1000);

BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_RANDOM)->Arg(10)->Arg(100)->Arg(1000);

BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace hyrise
