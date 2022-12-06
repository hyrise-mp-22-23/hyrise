#include "file_io_read_micro_benchmark.hpp"

namespace hyrise {

class FileIOReadMmapBenchmarkFixture : public FileIOMicroReadBenchmarkFixture {};

void read_mmap_chunk_sequential(const size_t from, const size_t to, const int32_t* map, uint64_t& sum) {
  for (auto index = size_t{0} + from; index < to; ++index) {
    sum += map[index];
  }
}

void read_mmap_chunk_random(const size_t from, const size_t to, const int32_t* map, uint64_t& sum,
                            const std::vector<uint32_t>& random_indexes) {
  for (auto index = size_t{0} + from; index < to; ++index) {
    sum += map[random_indexes[index]];
  }
}

void FileIOMicroReadBenchmarkFixture::mmap_read_single_threaded(benchmark::State& state, int mmap_mode_flag,
                                                                int access_order) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    auto* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, mmap_mode_flag, fd, OFFSET));
    Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));

    if (access_order == RANDOM) {
      madvise(map, NUMBER_OF_BYTES, MADV_RANDOM);
    } else {
      madvise(map, NUMBER_OF_BYTES, MADV_SEQUENTIAL);
    }
    auto sum = uint64_t{0};
    for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
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

void FileIOMicroReadBenchmarkFixture::mmap_read_multi_threaded(benchmark::State& state, int mmap_mode_flag,
                                                               uint16_t thread_count, int access_order) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  auto threads = std::vector<std::thread>(thread_count);
  auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    auto sums = std::vector<uint64_t>(thread_count);
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    auto* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, mmap_mode_flag, fd, OFFSET));
    Assert((map != MAP_FAILED), fail_and_close_file(fd, "Mapping Failed: ", errno));

    if (access_order == RANDOM) {
      state.PauseTiming();
      const auto random_indexes = generate_random_indexes(NUMBER_OF_ELEMENTS);
      state.ResumeTiming();

      madvise(map, NUMBER_OF_BYTES, MADV_RANDOM);

      for (auto i = size_t{0}; i < thread_count; i++) {
        auto from = batch_size * i;
        auto to = std::min(from + batch_size, uint64_t{NUMBER_OF_ELEMENTS});
        // std::ref fix from https://stackoverflow.com/a/73642536
        threads[i] = std::thread(read_mmap_chunk_random, from, to, map, std::ref(sums[i]), random_indexes);
      }
    } else {
      madvise(map, NUMBER_OF_BYTES, MADV_SEQUENTIAL);

      for (auto i = size_t{0}; i < thread_count; i++) {
        auto from = batch_size * i;
        auto to = std::min(from + batch_size, uint64_t{NUMBER_OF_ELEMENTS});
        // std::ref fix from https://stackoverflow.com/a/73642536
        threads[i] = std::thread(read_mmap_chunk_sequential, from, to, map, std::ref(sums[i]));
      }
    }
    for (auto i = size_t{0}; i < thread_count; i++) {
      // Blocks the current thread until the thread identified by *this finishes its execution
      threads[i].join();
    }
    state.PauseTiming();
    auto total_sum = std::accumulate(sums.begin(), sums.end(), uint64_t{0});

    Assert(control_sum == total_sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();

    Assert(msync(map, NUMBER_OF_BYTES, MS_SYNC) != -1, "Mapping Syncing Failed:" + std::strerror(errno));
    state.PauseTiming();

    // Remove memory mapping after job is done.
    Assert((munmap(map, NUMBER_OF_BYTES) == 0), fail_and_close_file(fd, "Unmapping failed: ", errno));
    state.ResumeTiming();
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_RANDOM)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    mmap_read_single_threaded(state, PRIVATE, RANDOM);
  } else {
    mmap_read_multi_threaded(state, PRIVATE, thread_count, RANDOM);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    mmap_read_single_threaded(state, PRIVATE, SEQUENTIAL);
  } else {
    mmap_read_multi_threaded(state, PRIVATE, thread_count, SEQUENTIAL);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    mmap_read_single_threaded(state, SHARED, RANDOM);
  } else {
    mmap_read_multi_threaded(state, SHARED, thread_count, RANDOM);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    mmap_read_single_threaded(state, SHARED, SEQUENTIAL);
  } else {
    mmap_read_multi_threaded(state, SHARED, thread_count, SEQUENTIAL);
  }
}

BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL)->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 32}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_RANDOM)->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 32}})
    ->UseRealTime();

BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 32}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 8, 16, 32}})
    ->UseRealTime();

}  // namespace hyrise
