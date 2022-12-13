#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <liburing.h>

#include <iterator>
#include <numeric>
#include <random>
#include <ostream>
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
    numbers = std::vector<uint32_t>(vector_element_count);
    for (auto index = size_t{0}; index < vector_element_count; ++index) {
      numbers[index] = std::rand() % UINT32_MAX;
    }
    control_sum = std::accumulate(numbers.begin(), numbers.end(), uint64_t{0});

    auto fd = int32_t{};
    if ((fd = open("file.txt", O_WRONLY | O_CREAT | O_TRUNC, S_IRWXG)) < 1) {
      std::cout << "create error " << fd << std::endl;
      exit(1);
    }
    chmod("file.txt", S_IRWXU);  // enables owner to rwx file
    //Assert(write(fd, std::data(numbers), BUFFER_SIZE_MB * MB != BUFFER_SIZE_MB * MB), "write error");
    if (write(fd, std::data(numbers), BUFFER_SIZE_MB * MB) != BUFFER_SIZE_MB * MB) {
      std::cout << "write error" << std::endl;
    }

    close(fd);
  }

  void TearDown(::benchmark::State& /*state*/) override {
    // TODO(everybody): Error handling
    //std::remove("file.txt");
  }

 protected:
};

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, READ_NON_ATOMIC_SEQUENTIAL)(benchmark::State& state) {  // open file
  auto fd = int32_t{};
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }

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

    for (auto index = size_t{0}; index < max_read_data_size; ++index) {
      lseek(fd, uint32_t_size * index, SEEK_SET);
      if (read(fd, std::data(read_data) + index, uint32_t_size) != uint32_t_size) {
        Fail("read error: " + strerror(errno));
      }
      //Assert(read(fd, read_data_start + index, uint32_t_size) != uint32_t_size, "read error: " + strerror(errno));
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
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }

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
    for (auto index = size_t{0}; index < max_read_data_size; ++index) {
      lseek(fd, uint32_t_size * random_indices[index], SEEK_SET);
      if (read(fd, read_data_start + index, uint32_t_size) != uint32_t_size) {
        Fail("read error: " + strerror(errno));
      }
      //Assert(read(fd, read_data_start + index, uint32_t_size) != uint32_t_size, "read error: " + strerror(errno));
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
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }

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

    for (auto index = size_t{0}; index < max_read_data_size; ++index) {
      if (pread(fd, read_data_start + index, uint32_t_size, uint32_t_size * index) != uint32_t_size) {
        Fail("read error: " + strerror(errno));
      }
      //Assert(pread(fd, read_data_start + index, uint32_t_size, uint32_t_size * index) != uint32_t_size, "read error: " + strerror(errno));
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
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }
  const uint32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    const auto random_indices = generate_random_indexes(vector_element_count);
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    int32_t* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_PRIVATE, fd, OFFSET));
    if (map == MAP_FAILED) {
      std::cout << "Mapping Failed. " << std::strerror(errno) << std::endl;
      continue;
    }

    madvise(map, NUMBER_OF_BYTES, MADV_RANDOM);

    auto sum = uint64_t{0};
    for (size_t index = 0; index < vector_element_count; ++index) {
      sum += map[random_indices[index]];
    }

    state.PauseTiming();
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();

    // Remove memory mapping after job is done.
    if (munmap(map, NUMBER_OF_BYTES) != 0) {
      std::cout << "Unmapping failed." << std::endl;
    }
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_PRIVATE_SEQUENTIAL)(benchmark::State& state) {
  auto fd = int32_t{};
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }
  const uint32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    int32_t* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_PRIVATE, fd, OFFSET));
    if (map == MAP_FAILED) {
      std::cout << "Mapping Failed. " << std::strerror(errno) << std::endl;
      continue;
    }

    madvise(map, NUMBER_OF_BYTES, MADV_SEQUENTIAL);

    auto sum = uint64_t{0};
    for (size_t index = 0; index < vector_element_count; ++index) {
      sum += map[index];
    }

    state.PauseTiming();
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();

    // Remove memory mapping after job is done.
    if (munmap(map, NUMBER_OF_BYTES) != 0) {
      std::cout << "Unmapping failed." << std::endl;
    }
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_RANDOM)(benchmark::State& state) {
  auto fd = int32_t{};
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }
  const uint32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    const auto random_indices = generate_random_indexes(vector_element_count);
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    int32_t* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_SHARED, fd, OFFSET));
    if (map == MAP_FAILED) {
      std::cout << "Mapping Failed. " << std::strerror(errno) << std::endl;
      continue;
    }

    madvise(map, NUMBER_OF_BYTES, MADV_RANDOM);

    auto sum = uint64_t{0};
    for (size_t index = 0; index < vector_element_count; ++index) {
      sum += map[random_indices[index]];
    }

    state.PauseTiming();
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();

    // Remove memory mapping after job is done.
    if (munmap(map, NUMBER_OF_BYTES) != 0) {
      std::cout << "Unmapping failed." << std::endl;
    }
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, MMAP_ATOMIC_MAP_SHARED_SEQUENTIAL)(benchmark::State& state) {
  auto fd = int32_t{};
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }
  const uint32_t NUMBER_OF_BYTES = state.range(0) * MB;

  for (auto _ : state) {
    state.PauseTiming();
    micro_benchmark_clear_disk_cache();
    state.ResumeTiming();

    // Getting the mapping to memory.
    const auto OFFSET = off_t{0};

    int32_t* map = reinterpret_cast<int32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_SHARED, fd, OFFSET));
    if (map == MAP_FAILED) {
      std::cout << "Mapping Failed. " << std::strerror(errno) << std::endl;
      continue;
    }

    madvise(map, NUMBER_OF_BYTES, MADV_SEQUENTIAL);

    auto sum = uint64_t{0};
    for (size_t index = 0; index < vector_element_count; ++index) {
      sum += map[index];
    }

    state.PauseTiming();
    Assert(control_sum == sum, "Sanity check failed: Not the same result");
    state.ResumeTiming();

    // Remove memory mapping after job is done.
    if (munmap(map, NUMBER_OF_BYTES) != 0) {
      std::cout << "Unmapping failed." << std::endl;
    }
  }

  close(fd);
}

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, PREAD_ATOMIC_RANDOM)(benchmark::State& state) {
  auto fd = int32_t{};
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }

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

    for (auto index = size_t{0}; index < max_read_data_size; ++index) {
      if (pread(fd, read_data_start + index, uint32_t_size, uint32_t_size * random_indices[index]) != uint32_t_size) {
        Fail("read error: " + strerror(errno));
      }
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
    std::cout << io_vectors[current_block].iov_len << std::endl;

    void *buf;
    if( posix_memalign(&buf, BLOCK_SZ, BLOCK_SZ)) {
      Fail("Could not allocate memory.");
    }
    io_vectors[current_block].iov_base = buf;

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

BENCHMARK_DEFINE_F(FileIOMicroReadBenchmarkFixture, IO_URING_READ_ASYNC)(benchmark::State& state) {  // open file
  const auto NUMBER_OF_BYTES = state.range(0) * MB;

  auto fd = int32_t{};
  if ((fd = open("file.txt", O_RDONLY | O_CLOEXEC)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }

  for (auto _ : state) {
    const auto queue_slots = 1;
    struct io_uring ring;
    io_uring_queue_init(queue_slots, &ring, 0);
    auto finfo = get_file_info(&fd, &ring, NUMBER_OF_BYTES);

    auto current_block = uint32_t{0};
    while (current_block < finfo.blocks) {

      // Get an SQE
      struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
      const iovec curr_iovec = finfo.io_vectors[current_block];
      // Setup a read operation
      io_uring_prep_readv(sqe, fd, &curr_iovec, 1, 0);
      // Set user data
      io_uring_sqe_set_data(sqe, &finfo.io_vectors[current_block]);
      // Finally, submit the request
      io_uring_submit(&ring);

      // Wait for CQE
      // Note: This is not efficient. Submitting more SQEs with more Ring Slots is the way of the async warrior.
      struct io_uring_cqe *cqe;
      auto ret = io_uring_wait_cqe(&ring, &cqe);
      if (ret < 0) {
        Fail("Could not wait for CQE");
      }
      if (cqe->res < 0) {
        Fail("Async Read did not succeed.");
      }
      ++current_block;

    }
    io_uring_queue_exit(&ring);
  }
}

// Arguments are file size in MB
/*
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
*/
BENCHMARK_REGISTER_F(FileIOMicroReadBenchmarkFixture, IO_URING_READ_ASYNC)->Arg(10)->Arg(100)->Arg(1000);


}  // namespace hyrise
