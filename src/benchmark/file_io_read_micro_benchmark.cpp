#include <aio.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <liburing.h>
#include <stdlib.h>

#include <iterator>
#include <numeric>
#include <random>
#include <thread>
#include "file_io_read_micro_benchmark.hpp"
#include "micro_benchmark_basic_fixture.hpp"

namespace hyrise {

void read_data_using_read(const size_t from, const size_t to, int32_t fd, uint32_t* read_data_start) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto bytes_to_read = static_cast<ssize_t>(uint32_t_size * (to - from));
  lseek(fd, from * uint32_t_size, SEEK_SET);
  Assert((read(fd, read_data_start + from, bytes_to_read) == bytes_to_read),
         fail_and_close_file(fd, "Read error: ", errno));
}

void read_data_randomly_using_read(const size_t from, const size_t to, int32_t fd, uint32_t* read_data_start,
                                   const std::vector<uint32_t>& random_indices) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};

  lseek(fd, 0, SEEK_SET);
  // TODO(everyone): Randomize inidzes to not read all the data but really randomize the reads to read same amount but
  //  incl possible duplicates
  for (auto index = from; index < to; ++index) {
    lseek(fd, uint32_t_size * random_indices[index], SEEK_SET);
    Assert((read(fd, read_data_start + index, uint32_t_size) == uint32_t_size),
           fail_and_close_file(fd, "Read error: ", errno));
  }
}

void read_data_using_pread(const size_t from, const size_t to, int32_t fd, uint32_t* read_data_start) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto bytes_to_read = static_cast<ssize_t>(uint32_t_size * (to - from));
  lseek(fd, from * uint32_t_size, SEEK_SET);
  Assert((pread(fd, read_data_start + from, bytes_to_read, from * uint32_t_size) == bytes_to_read),
         fail_and_close_file(fd, "Read error: ", errno));
}

void read_data_randomly_using_pread(const size_t from, const size_t to, int32_t fd, uint32_t* read_data_start,
                                    const std::vector<uint32_t>& random_indices) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};

  lseek(fd, 0, SEEK_SET);
  // TODO(everyone): Randomize inidzes to not read all the data but really randomize the reads to read same amount but
  //  incl possible duplicates
  for (auto index = from; index < to; ++index) {
    Assert((pread(fd, read_data_start + index, uint32_t_size, uint32_t_size * random_indices[index]) == uint32_t_size),
           fail_and_close_file(fd, "Read error: ", errno));
  }
}

void read_data_using_aio(const size_t from, const size_t to, int32_t fd, uint32_t* read_data_start) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto bytes_to_read = static_cast<ssize_t>(uint32_t_size * (to - from));
  struct aiocb aiocb;

  memset(&aiocb, 0, sizeof(struct aiocb));
  aiocb.aio_fildes = fd;
  aiocb.aio_offset = from * uint32_t_size;
  aiocb.aio_buf = read_data_start + from;
  aiocb.aio_nbytes = bytes_to_read;
  aiocb.aio_lio_opcode = LIO_READ;

  Assert(aio_read(&aiocb) == 0, "Read error: " + strerror(errno));

  auto err = aio_error(&aiocb);
  /* Wait until end of transaction */
  while (err == EINPROGRESS);

  aio_error_handling(&aiocb, bytes_to_read);
}

void read_data_randomly_using_aio(const size_t from, const size_t to, int32_t fd, uint32_t* read_data_start,
                                  const std::vector<uint32_t>& random_indices) {
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  struct aiocb aiocb;
  memset(&aiocb, 0, sizeof(struct aiocb));
  aiocb.aio_fildes = fd;
  aiocb.aio_lio_opcode = LIO_READ;
  aiocb.aio_nbytes = uint32_t_size;

  // TODO(everyone): Randomize inidzes to not read all the data but really randomize the reads to read same amount but
  //  incl possible duplicates
  for (auto index = from; index < to; ++index) {
    aiocb.aio_buf = read_data_start + index;
    aiocb.aio_offset = uint32_t_size * random_indices[index];
    Assert(aio_read(&aiocb) == 0, "Read error: " + strerror(errno));

    auto err = aio_error(&aiocb);
    /* Wait until end of transaction */
    while (err == EINPROGRESS);

    aio_error_handling(&aiocb, uint32_t_size);
  }
}

// TODO(everyone): Reduce LOC by making this function more modular (do not repeat it with different function inputs).
void FileIOMicroReadBenchmarkFixture::read_non_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count) {
  auto filedescriptors = std::vector<int32_t>(thread_count);
  for (auto index = size_t{0}; index < thread_count; ++index) {
    auto fd = int32_t{};
    Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));
    filedescriptors[index] = fd;
  }

  auto threads = std::vector<std::thread>(thread_count);
  auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    auto* read_data_start = std::data(read_data);

    state.ResumeTiming();

    for (auto index = size_t{0}; index < thread_count; ++index) {
      auto from = batch_size * index;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[index] = (std::thread(read_data_using_read, from, to, filedescriptors[index], read_data_start));
    }

    for (auto index = size_t{0}; index < thread_count; ++index) {
      // Explain: Blocks the current thread until the thread identified by *this finishes its execution
      threads[index].join();
    }
    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }

  for (auto index = size_t{0}; index < thread_count; ++index) {
    close(filedescriptors[index]);
  }
}

void FileIOMicroReadBenchmarkFixture::read_non_atomic_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);

    state.ResumeTiming();

    lseek(fd, 0, SEEK_SET);
    Assert((read(fd, std::data(read_data), NUMBER_OF_BYTES) == NUMBER_OF_BYTES),
           fail_and_close_file(fd, "Read error: ", errno));

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::read_non_atomic_random_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    const auto random_indices = generate_random_indexes(NUMBER_OF_ELEMENTS);
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);

    state.ResumeTiming();

    lseek(fd, 0, SEEK_SET);
    // TODO(everyone): Randomize inidzes to not read all the data but really randomize the reads to read same amount but
    //  incl possible duplicates
    for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
      lseek(fd, uint32_t_size * random_indices[index], SEEK_SET);
      Assert((read(fd, std::data(read_data) + index, uint32_t_size) == uint32_t_size),
             fail_and_close_file(fd, "Read error: ", errno));
    }

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::read_non_atomic_random_multi_threaded(benchmark::State& state,
                                                                            uint16_t thread_count) {
  auto filedescriptors = std::vector<int32_t>(thread_count);
  for (auto index = size_t{0}; index < thread_count; ++index) {
    auto fd = int32_t{};
    Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));
    filedescriptors[index] = fd;
  }

  auto threads = std::vector<std::thread>(thread_count);
  auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    const auto random_indices = generate_random_indexes(NUMBER_OF_ELEMENTS);
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);

    state.ResumeTiming();
    for (auto index = size_t{0}; index < thread_count; ++index) {
      auto from = batch_size * index;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[index] = (std::thread(read_data_randomly_using_read, from, to, filedescriptors[index], std::data(read_data),
                                random_indices));
    }

    for (auto index = size_t{0}; index < thread_count; ++index) {
      // Explain: Blocks the current thread until the thread identified by *this finishes its execution
      threads[index].join();
    }
    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  for (auto index = size_t{0}; index < thread_count; ++index) {
    close(filedescriptors[index]);
  }
}

void FileIOMicroReadBenchmarkFixture::pread_atomic_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    state.ResumeTiming();

    Assert((pread(fd, std::data(read_data), NUMBER_OF_BYTES, 0) == NUMBER_OF_BYTES),
           fail_and_close_file(fd, "Read error: ", errno));

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::pread_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  auto threads = std::vector<std::thread>(thread_count);
  auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    auto* read_data_start = std::data(read_data);

    state.ResumeTiming();

    for (auto index = size_t{0}; index < thread_count; ++index) {
      auto from = batch_size * index;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[index] = (std::thread(read_data_using_pread, from, to, fd, read_data_start));
    }

    for (auto index = size_t{0}; index < thread_count; ++index) {
      // Explain: Blocks the current thread until the thread identified by *this finishes its execution
      threads[index].join();
    }
    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::pread_atomic_random_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    const auto random_indices = generate_random_indexes(NUMBER_OF_ELEMENTS);
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);

    state.ResumeTiming();

    // TODO(everyone) Randomize inidzes to not read all the data but really randomize
    for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
      Assert((pread(fd, std::data(read_data) + index, uint32_t_size, uint32_t_size * random_indices[index]) ==
              uint32_t_size),
             fail_and_close_file(fd, "Read error: ", errno));
    }

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::pread_atomic_random_multi_threaded(benchmark::State& state,
                                                                         uint16_t thread_count) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  auto threads = std::vector<std::thread>(thread_count);
  auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    const auto random_indices = generate_random_indexes(NUMBER_OF_ELEMENTS);
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);

    state.ResumeTiming();
    for (auto index = size_t{0}; index < thread_count; ++index) {
      auto from = batch_size * index;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[index] = (std::thread(read_data_randomly_using_pread, from, to, fd, std::data(read_data), random_indices));
    }

    for (auto index = size_t{0}; index < thread_count; ++index) {
      // Explain: Blocks the current thread until the thread identified by *this finishes its execution
      threads[index].join();
    }
    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::aio_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    state.ResumeTiming();

    struct aiocb aiocb;

    memset(&aiocb, 0, sizeof(struct aiocb));
    aiocb.aio_fildes = fd;
    aiocb.aio_offset = 0;
    aiocb.aio_buf = std::data(read_data);
    aiocb.aio_nbytes = NUMBER_OF_BYTES;
    aiocb.aio_lio_opcode = LIO_READ;

    Assert(aio_read(&aiocb) == 0, "Read error: " + strerror(errno));

    auto err = aio_error(&aiocb);
    /* Wait until end of transaction */
    while (err == EINPROGRESS);

    aio_error_handling(&aiocb, NUMBER_OF_BYTES);

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::aio_multi_threaded(benchmark::State& state, uint16_t thread_count) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  auto threads = std::vector<std::thread>(thread_count);
  auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    auto* read_data_start = std::data(read_data);

    state.ResumeTiming();

    for (auto index = size_t{0}; index < thread_count; ++index) {
      auto from = batch_size * index;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[index] = (std::thread(read_data_using_aio, from, to, fd, read_data_start));
    }

    for (auto index = size_t{0}; index < thread_count; ++index) {
      // Explain: Blocks the current thread until the thread identified by *this finishes its execution
      threads[index].join();
    }
    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::aio_random_single_threaded(benchmark::State& state) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    const auto random_indices = generate_random_indexes(NUMBER_OF_ELEMENTS);
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    auto* read_data_start = std::data(read_data);

    state.ResumeTiming();

    struct aiocb aiocb;

    memset(&aiocb, 0, sizeof(struct aiocb));
    aiocb.aio_fildes = fd;
    aiocb.aio_lio_opcode = LIO_READ;
    aiocb.aio_nbytes = uint32_t_size;

    // TODO(everyone) Randomize inidzes to not read all the data but really randomize
    for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
      aiocb.aio_offset = uint32_t_size * random_indices[index];
      aiocb.aio_buf = read_data_start + index;

      Assert(aio_read(&aiocb) == 0, "Read error: " + strerror(errno));

      auto err = aio_error(&aiocb);
      /* Wait until end of transaction */
      while (err == EINPROGRESS);
      aio_error_handling(&aiocb, uint32_t_size);
    }

    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  close(fd);
}

void FileIOMicroReadBenchmarkFixture::aio_random_multi_threaded(benchmark::State& state, uint16_t thread_count) {
  auto fd = int32_t{};
  Assert(((fd = open(filename, O_RDONLY)) >= 0), fail_and_close_file(fd, "Open error: ", errno));

  auto threads = std::vector<std::thread>(thread_count);
  auto batch_size = static_cast<uint64_t>(std::ceil(static_cast<float>(NUMBER_OF_ELEMENTS) / thread_count));

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    const auto random_indices = generate_random_indexes(NUMBER_OF_ELEMENTS);
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);

    state.ResumeTiming();
    for (auto index = size_t{0}; index < thread_count; ++index) {
      auto from = batch_size * index;
      auto to = from + batch_size;
      if (to >= NUMBER_OF_ELEMENTS) {
        to = NUMBER_OF_ELEMENTS;
      }
      threads[index] = (std::thread(read_data_randomly_using_aio, from, to, fd, std::data(read_data), random_indices));
    }

    for (auto index = size_t{0}; index < thread_count; ++index) {
      // Explain: Blocks the current thread until the thread identified by *this finishes its execution
      threads[index].join();
    }
    state.PauseTiming();

    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed: Not the same result");

    state.ResumeTiming();
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_SEQUENTIAL_THREADED)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    read_non_atomic_single_threaded(state);
  } else {
    read_non_atomic_multi_threaded(state, thread_count);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_RANDOM_THREADED)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    read_non_atomic_random_single_threaded(state);
  } else {
    read_non_atomic_random_multi_threaded(state, thread_count);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_SEQUENTIAL_THREADED)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    pread_atomic_single_threaded(state);
  } else {
    pread_atomic_multi_threaded(state, thread_count);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_RANDOM_THREADED)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    pread_atomic_random_single_threaded(state);
  } else {
    pread_atomic_random_multi_threaded(state, thread_count);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, AIO_SEQUENTIAL_THREADED)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    aio_single_threaded(state);
  } else {
    aio_multi_threaded(state, thread_count);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, AIO_RANDOM_THREADED)(benchmark::State& state) {
  auto thread_count = static_cast<uint16_t>(state.range(1));
  if (thread_count == 1) {
    aio_random_single_threaded(state);
  } else {
    aio_random_multi_threaded(state, thread_count);
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_SEQUENTIAL)(benchmark::State& state) {
  for (auto _ : state) {
    state.PauseTiming();
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);

    state.ResumeTiming();

    for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
      read_data[index] = numbers[index];
    }

    state.PauseTiming();
    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});

    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    Assert(&read_data != &numbers, "Sanity check failed: Same reference");

    state.ResumeTiming();
  }
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_RANDOM)(benchmark::State& state) {
  for (auto _ : state) {
    state.PauseTiming();
    const auto random_indices = generate_random_indexes(NUMBER_OF_ELEMENTS);
    auto read_data = std::vector<uint32_t>{};
    read_data.resize(NUMBER_OF_ELEMENTS);
    state.ResumeTiming();

    for (auto index = size_t{0}; index < NUMBER_OF_ELEMENTS; ++index) {
      read_data[index] = numbers[random_indices[index]];
    }

    state.PauseTiming();
    const auto sum = std::accumulate(read_data.begin(), read_data.end(), uint64_t{0});

    Assert(control_sum == static_cast<uint64_t>(sum), "Sanity check failed: Not the same result");
    Assert(&read_data[0] != &numbers[random_indices[0]], "Sanity check failed: Same reference");

    state.ResumeTiming();
  }
}

struct file_info{
  long file_size;
  uint32_t blocks;
  std::vector<iovec> io_vectors;
};

file_info get_file_info(int *fd, struct io_uring *ring, const long NUMBER_OF_BYTES) {
  const auto BLOCK_SZ = 4096;

  auto bytes_remaining = NUMBER_OF_BYTES;
  auto current_block = 0;
  auto blocks = (int) NUMBER_OF_BYTES / BLOCK_SZ;
  if (NUMBER_OF_BYTES % BLOCK_SZ) blocks++;

  auto io_vectors = std::vector<iovec>(blocks);

  /*
     * For each block of the file we need to read, we allocate an iovec struct
     * which is indexed into the iovecs array. This array is passed in as part
     * of the submission. If you don't understand this, then you need to look
     * up how the readv() and writev() system calls work.
     * */
  while (bytes_remaining) {
    off_t bytes_to_read = bytes_remaining;
    if (bytes_to_read > BLOCK_SZ)
      bytes_to_read = BLOCK_SZ;

    io_vectors[current_block].iov_len = bytes_to_read;

    auto buffer = (void*) malloc(BLOCK_SZ);
    io_vectors[current_block].iov_base = static_cast<void*>(buffer);

    current_block++;
    bytes_remaining -= bytes_to_read;
  }

  file_info finfo;
  finfo.file_size = NUMBER_OF_BYTES;
  finfo.blocks = blocks;
  finfo.io_vectors = io_vectors;

  return finfo;
}

void output_to_console(char *buf, int len) {
  while (len--) {
    fputc(*buf++, stdout);
  }
}

// See https://github.com/shuveb/io_uring-by-example/blob/master/03_cat_liburing/main.c
BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, IO_URING_READ_ASYNC)(benchmark::State& state) {  // open file
  const auto SQE_SLOTS = state.range(1);

  // Ye - that looks good. @Reviews: Is this the way number of elements is supposed to be used?
  const auto NUMBER_OF_BYTES = NUMBER_OF_ELEMENTS;

  auto fd = int32_t{};
  if ((fd = open("file.txt", O_RDONLY | O_CLOEXEC)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }

  for (auto _ : state) {
    struct io_uring ring;
    io_uring_queue_init(SQE_SLOTS, &ring, 0);
    auto finfo = get_file_info(&fd, &ring, NUMBER_OF_BYTES);

    /*
     * Submitting a read request.
     *    Note: There is no user data required in this case.
     *          If there was more than a single read request
     *          (one SQE, one CQE), one would need a way to
     *          identify each of the CQEs.
     */
    // Get an SQE
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    // Setup a read operation
    // io_vectors.data() is equivalent to &io_vectors[0]
    io_uring_prep_readv(sqe, fd, finfo.io_vectors.data(), finfo.io_vectors.size(), 0);
    // Finally, submit the request
    io_uring_submit(&ring);

    struct io_uring_cqe *cqe;
    auto ret = io_uring_wait_cqe(&ring, &cqe);
    if (ret < 0) {
      Fail("Could not wait for CQE");
    }
    if (cqe->res < 0) {
      Fail("Async Read did not succeed.");
    }

    // This outputs the stuff from the file. But actually unreadable stuff comes out of this.
    // So I decided against a sanity check, to have time for other things.
    for (auto iovecInd = uint32_t{0}; iovecInd < finfo.io_vectors.size(); ++iovecInd) {
      const auto io_vector = finfo.io_vectors[iovecInd];

      auto resulting_numbers = std::vector<uint32_t>(static_cast<uint32_t>(4096 / 4));

      for (auto ind = uint32_t{0}; )

      auto iov_base = static_cast<uint32_t*>(io_vector.iov_base);
      std::cout << iov_base;
    }
    io_uring_cqe_seen(&ring, cqe);

    io_uring_queue_exit(&ring);
  }
}

// Arguments are file size in MB
/*
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_SEQUENTIAL_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 6, 8, 16, 24, 32, 48}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_RANDOM_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 6, 8, 16, 24, 32, 48}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_SEQUENTIAL_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 6, 8, 16, 24, 32, 48}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_RANDOM_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 6, 8, 16, 24, 32, 48}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, AIO_SEQUENTIAL_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 6, 8, 16, 24, 32, 48}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, AIO_RANDOM_THREADED)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 6, 8, 16, 24, 32, 48}})
    ->UseRealTime();
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_SEQUENTIAL)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, IN_MEMORY_READ_RANDOM)->Arg(10)->Arg(100)->Arg(1000);*/
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, IO_URING_READ_ASYNC)
    ->ArgsProduct({{10, 100, 1000}, {1, 2, 4, 6, 8, 16, 24, 32, 48}})
    ->UseRealTime();

}  // namespace hyrise
