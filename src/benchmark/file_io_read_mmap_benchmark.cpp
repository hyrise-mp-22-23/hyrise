#include "file_io_read_micro_benchmark.hpp"

#ifdef __linux__
#include <umap/umap.h>
#endif

namespace {

// Worker function for threading.
void read_mmap_chunk_sequential(const size_t from, const size_t to, const uint32_t* map,
                                std::vector<uint32_t>& read_data, std::atomic<bool>& threads_ready_to_be_executed) {
  while (!threads_ready_to_be_executed) {}
  memcpy(std::data(read_data) + from, map + from, (to - from) * sizeof(uint32_t));
}

// Worker function for threading.
void read_mmap_chunk_random(const size_t from, const size_t to, const uint32_t* map, uint64_t& sum,
                            const std::vector<uint32_t>& random_indexes, std::atomic<bool>& threads_ready_to_be_executed) {
  while (!threads_ready_to_be_executed) {}

  for (auto index = size_t{0} + from; index < to; ++index) {
    sum += map[random_indexes[index]];
  }
}

}  // namespace

namespace hyrise {

void FileIOMicroReadBenchmarkFixture::memory_mapped_read_single_threaded(benchmark::State& state,
                                                                         const int mapping_type,
                                                                         const int map_mode_flag,
                                                                         const int access_order) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), close_file_and_return_error_message(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, map_mode_flag, fd, OFFSET));

    if (mapping_type == MMAP) {
      map = reinterpret_cast<uint32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, map_mode_flag, fd, OFFSET));
    }
#ifdef __APPLE__
    else {
      Fail("Error: Mapping type invalid or not supported on Apple platforms.");
    }
#else
    else if (mapping_type == UMAP) {
      setenv("UMAP_LOG_LEVEL", std::string("ERROR").c_str(), 1);
      map = reinterpret_cast<uint32_t*>(umap(NULL, NUMBER_OF_BYTES, PROT_READ, map_mode_flag, fd, OFFSET));
    } else {
      Fail("Error: Invalid mapping type.");
    }
#endif

    Assert((map != MAP_FAILED), close_file_and_return_error_message(fd, "Mapping Failed: ", errno));

    auto sum = uint64_t{0};
    if (access_order == RANDOM) {
      madvise(map, NUMBER_OF_BYTES, MADV_RANDOM);
      state.PauseTiming();
      const auto random_indexes = generate_random_indexes(NUMBER_OF_ELEMENTS);
      state.ResumeTiming();
      for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
        sum += map[random_indexes[index]];
      }
    } else /* if (access_order == SEQUENTIAL) */ {
      madvise(map, NUMBER_OF_BYTES, MADV_SEQUENTIAL);

      memcpy(std::data(read_data), map, NUMBER_OF_BYTES);
    }

    state.PauseTiming();
    if (access_order == SEQUENTIAL) {
      sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    }
    Assert(control_sum == sum, "Sanity check failed: Not the same result. Got: " + std::to_string(sum) +
                                   " Expected: " + std::to_string(control_sum) + ".");
    state.ResumeTiming();

    // Remove memory mapping after job is done.
    if (mapping_type == MMAP) {
      Assert((munmap(map, NUMBER_OF_BYTES) == 0), close_file_and_return_error_message(fd, "Unmapping failed: ", errno));
    }
#ifdef __APPLE__
    else {
      Fail("Error: Mapping type invalid or not supported on Apple platforms.");
    }
#else
    else /* if (mapping_type == UMAP) */ {
      Assert((uunmap(map, NUMBER_OF_BYTES) == 0), close_file_and_return_error_message(fd, "Unmapping failed: ", errno));
    }
#endif
  }
  close(fd);
}

#ifdef __linux__
void FileIOMicroReadBenchmarkFixture::memory_mapped_read_user_space(benchmark::State& state, const uint16_t thread_count,
  const int access_order) {
  // Set number of threads used by UMAP.
  setenv("UMAP_PAGE_FILLERS", std::to_string(thread_count).c_str(), 1);
  setenv("UMAP_PAGE_EVICTORS", std::to_string(thread_count).c_str(), 1);
  setenv("UMAP_LOG_LEVEL", std::string("ERROR").c_str(), 1);

  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), close_file_and_return_error_message(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    auto* map = reinterpret_cast<uint32_t*>(umap(NULL, NUMBER_OF_BYTES, PROT_READ, PRIVATE, fd, OFFSET));


    Assert((map != MAP_FAILED), close_file_and_return_error_message(fd, "Mapping Failed: ", errno));

    auto sum = uint64_t{0};
    if (access_order == RANDOM) {
      state.PauseTiming();
      const auto random_indexes = generate_random_indexes(NUMBER_OF_ELEMENTS);
      state.ResumeTiming();
      for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
        sum += map[random_indexes[index]];
      }
    } else /* if (access_order == SEQUENTIAL) */ {
      memcpy(std::data(read_data), map, NUMBER_OF_BYTES);
    }

    state.PauseTiming();
    if (access_order == SEQUENTIAL){
      for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
        sum += read_data[index];
      }
    }
    Assert(control_sum == sum, "Sanity check failed: Not the same result. Got: " + std::to_string(sum) +
                                   " Expected: " + std::to_string(control_sum) + ".");
    state.ResumeTiming();

    // Remove memory mapping after job is done.
    Assert((uunmap(map, NUMBER_OF_BYTES) == 0), close_file_and_return_error_message(fd, "Unmapping failed: ", errno));
  }

  close(fd);
}
#endif

void FileIOMicroReadBenchmarkFixture::memory_mapped_read_multi_threaded(benchmark::State& state, const int mapping_type,
                                                                        const int map_mode_flag,
                                                                        const uint16_t thread_count,
                                                                        const int access_order) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), close_file_and_return_error_message(fd, "Open error: ", errno));

  auto threads = std::vector<std::thread>(thread_count);
  const auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    auto sums = std::vector<uint64_t>(thread_count);
    std::atomic<bool> threads_ready_to_be_executed = false;
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, map_mode_flag, fd, OFFSET));

    if (mapping_type == MMAP) {
      map = reinterpret_cast<uint32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, map_mode_flag, fd, OFFSET));
    }
#ifdef __APPLE__
    else {
      Fail("Error: Mapping type invalid or not supported on Apple platforms.");
    }
#else
    else if (mapping_type == UMAP) {
      setenv("UMAP_LOG_LEVEL", std::string("ERROR").c_str(), 1);
      map = reinterpret_cast<uint32_t*>(umap(NULL, NUMBER_OF_BYTES, PROT_READ, map_mode_flag, fd, OFFSET));
    } else {
      Fail("Error: Invalid mapping type.");
    }
#endif

    Assert((map != MAP_FAILED), close_file_and_return_error_message(fd, "Mapping Failed: ", errno));

    if (access_order == RANDOM) {
      state.PauseTiming();
      const auto random_indexes = generate_random_indexes(NUMBER_OF_ELEMENTS);
      state.ResumeTiming();

      if (mapping_type == MMAP) {
        madvise(map, NUMBER_OF_BYTES, MADV_RANDOM);
      }
      state.PauseTiming();

      for (auto i = size_t{0}; i < thread_count; ++i) {
        const auto from = batch_size * i;
        const auto to = std::min(from + batch_size, uint64_t{NUMBER_OF_ELEMENTS});
        // std::ref fix from https://stackoverflow.com/a/73642536
        threads[i] = std::thread(read_mmap_chunk_random, from, to, map, std::ref(sums[i]), random_indexes,
                                 std::ref(threads_ready_to_be_executed));
      }
    } else {
      if (mapping_type == MMAP) {
        madvise(map, NUMBER_OF_BYTES, MADV_SEQUENTIAL);
      }
      state.PauseTiming();

      for (auto i = size_t{0}; i < thread_count; ++i) {
        const auto from = batch_size * i;
        const auto to = std::min(from + batch_size, uint64_t{NUMBER_OF_ELEMENTS});
        // std::ref fix from https://stackoverflow.com/a/73642536
        threads[i] = std::thread(read_mmap_chunk_sequential, from, to, map, std::ref(read_data),
                                 std::ref(threads_ready_to_be_executed));
      }
    }

    state.ResumeTiming();
    threads_ready_to_be_executed = true;

    for (auto index = size_t{0}; index < thread_count; ++index) {
      // Blocks the current thread until the thread identified by *this finishes its execution
      threads[index].join();
    }
    state.PauseTiming();
    auto sum = uint64_t{0};
    if (access_order == SEQUENTIAL) {
      sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    } else {
      sum = std::accumulate(sums.begin(), sums.end(), uint64_t{0});
    }
    Assert(control_sum == sum, "Sanity check failed: Not the same result. Got: " + std::to_string(sum) +
                                   " Expected: " + std::to_string(control_sum) + ".");

    state.ResumeTiming();

    if (mapping_type == MMAP) {
      Assert(msync(map, NUMBER_OF_BYTES, MS_SYNC) != -1, "Mapping Syncing Failed:" + std::strerror(errno));
    }

    state.PauseTiming();

    // Remove memory mapping after job is done.
    if (mapping_type == MMAP) {
      Assert((munmap(map, NUMBER_OF_BYTES) == 0), close_file_and_return_error_message(fd, "Unmapping failed: ", errno));
    }
#ifdef __APPLE__
    else {
      Fail("Error: Mapping type invalid or not supported on Apple platforms.");
    }
#else
    else /* if (mapping_type == UMAP) */ {
      Assert((uunmap(map, NUMBER_OF_BYTES) == 0), close_file_and_return_error_message(fd, "Unmapping failed: ", errno));
    }
#endif
    state.ResumeTiming();
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_RANDOM)(benchmark::State& state) {
  const auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    memory_mapped_read_single_threaded(state, MMAP, PRIVATE, RANDOM);
  } else {
    memory_mapped_read_multi_threaded(state, MMAP, PRIVATE, thread_count, RANDOM);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL)(benchmark::State& state) {
  const auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    memory_mapped_read_single_threaded(state, MMAP, PRIVATE, SEQUENTIAL);
  } else {
    memory_mapped_read_multi_threaded(state, MMAP, PRIVATE, thread_count, SEQUENTIAL);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)(benchmark::State& state) {
  const auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    memory_mapped_read_single_threaded(state, MMAP, SHARED, RANDOM);
  } else {
    memory_mapped_read_multi_threaded(state, MMAP, SHARED, thread_count, RANDOM);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)(benchmark::State& state) {
  const auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    memory_mapped_read_single_threaded(state, MMAP, SHARED, SEQUENTIAL);
  } else {
    memory_mapped_read_multi_threaded(state, MMAP, SHARED, thread_count, SEQUENTIAL);
  }
}

#ifdef __linux__
BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, UMAP_ATOMIC_MAP_PRIVATE_RANDOM)(benchmark::State& state) {
  const auto thread_count = static_cast<uint16_t>(state.range(1));
  memory_mapped_read_user_space(state, thread_count, RANDOM);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, UMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL)(benchmark::State& state) {
  const auto thread_count = static_cast<uint16_t>(state.range(1));
  memory_mapped_read_user_space(state, thread_count, SEQUENTIAL);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, UMAP_ATOMIC_MAP_PRIVATE_RANDOM_OLD)(benchmark::State& state) {
  const auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
     memory_mapped_read_single_threaded(state, UMAP, PRIVATE, RANDOM);
   } else {
     memory_mapped_read_multi_threaded(state, UMAP, PRIVATE, thread_count, RANDOM);
   }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, UMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL_OLD)(benchmark::State& state) {
  const auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    memory_mapped_read_single_threaded(state, UMAP, PRIVATE, RANDOM);
  } else {
    memory_mapped_read_multi_threaded(state, UMAP, PRIVATE, thread_count, RANDOM);
  }
}
#endif

BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL)
    ->ArgsProduct({{1000}, {1, 2, 4, 8, 16, 32, 64}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_RANDOM)
  ->ArgsProduct({{1000}, {1, 2, 4, 8, 16, 32, 64}})
    ->UseRealTime();

BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)
  ->ArgsProduct({{1000}, {1, 2, 4, 8, 16, 32, 64}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)
    ->ArgsProduct({{1000}, {1, 2, 4, 8, 16, 32, 64}})
    ->UseRealTime();

#ifdef __linux__
// BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, UMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL)
//    ->ArgsProduct({{1000}, {1, 2}})
//    ->UseRealTime();

// BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, UMAP_ATOMIC_MAP_PRIVATE_RANDOM)
//    ->ArgsProduct({{1000}, {1, 2}})
//    ->UseRealTime();

// BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, UMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL_OLD)
//    ->ArgsProduct({{1000}, {1, 2}})
//    ->UseRealTime();

// BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, UMAP_ATOMIC_MAP_PRIVATE_RANDOM_OLD)
//    ->ArgsProduct({{1000}, {1, 2}})
//    ->UseRealTime();
#endif

}  // namespace hyrise
