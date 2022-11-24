#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <iterator>
#include <numeric>
#include <random>
#include <thread>

#include <aio.h>
#include "micro_benchmark_basic_fixture.hpp"

namespace hyrise {

const auto MB = uint32_t{1'000'000};

class FileAsyncIOMicroWriteBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  uint64_t control_sum = uint64_t{0};
  std::vector<uint32_t> numbers = std::vector<uint32_t>{};
  uint32_t vector_element_count;

  void SetUp(::benchmark::State& state) override {
    // TODO(everybody): Make setup/teardown global per file size to improve benchmark speed
    ssize_t BUFFER_SIZE_MB = state.range(0);

    // each int32_t contains four bytes
    vector_element_count = (BUFFER_SIZE_MB * MB) / sizeof(uint32_t);
    numbers = std::vector<uint32_t>(vector_element_count);
    for (size_t index = 0; index < vector_element_count; ++index) {
      numbers[index] = std::rand() % UINT32_MAX;
    }
    control_sum = std::accumulate(numbers.begin(), numbers.end(), uint64_t{0});

    int32_t fd;
    if ((fd = creat("file.txt", O_WRONLY)) < 1) {
      std::cout << "create error" << std::endl;
    }
    //Assert((fd = creat("file.txt", O_WRONLY)) < 1, "create error");
    chmod("file.txt", S_IRWXU);  // enables owner to rwx file
    //Assert(write(fd, std::data(numbers), BUFFER_SIZE_MB * MB != BUFFER_SIZE_MB * MB), "write error");
    if (write(fd, std::data(numbers), BUFFER_SIZE_MB * MB) != BUFFER_SIZE_MB * MB) {
      std::cout << "write error" << std::endl;
    }

    close(fd);
  }

  void aio_write_error_handling(aiocb* aiocb, uint32_t EXPECTED_BYTES){
    const auto err = aio_error(aiocb);
    const auto ret = aio_return(aiocb);

    if (err != 0) {
      std::cout << "Error at aio_error(): " << std::strerror(errno) << std::endl;
      close(aiocb->aio_fildes);
      exit(2);
    }

    if (ret != static_cast<int32_t>(EXPECTED_BYTES)) {
      std::cout << "Error at aio_return(). Got: " << ret << " Expected: " << EXPECTED_BYTES << std::endl;
      close(aiocb->aio_fildes);
      exit(2);
    }
  }

  void TearDown(::benchmark::State& /*state*/) override {
    // TODO(everybody): Error handling
    std::remove("file.txt");
  }

 protected:
};

BENCHMARK_DEFINE_F(FileAsyncIOMicroWriteBenchmarkFixture, AIO_WRITE_SEQUENTIAL)(benchmark::State& state) {  // open file
  int32_t fd;
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }

  const auto NUMBER_OF_BYTES = static_cast<uint32_t>(state.range(0) * MB);
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto write_data_size = NUMBER_OF_BYTES / uint32_t_size;


  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    auto write_data = std::vector<uint32_t>{};
    write_data.resize(write_data_size);

    state.ResumeTiming();

    // The standard aio control block. A structure, that holds the information about the asnyc IO op.
    struct aiocb aiocb;
    memset(&aiocb, 0, sizeof(struct aiocb));
    aiocb.aio_fildes = fd;
    aiocb.aio_buf = std::data(write_data);
    aiocb.aio_nbytes = 200;
    aiocb.aio_lio_opcode = LIO_WRITE;

    if (aio_write(&aiocb) == -1) {
      Fail("write error: " + strerror(errno));
    }
    
    auto err = aio_error(&aiocb);
    /* Wait until end of transaction */
    while (err == EINPROGRESS) {
      err = aio_error(&aiocb);
    }

    aio_write_error_handling(&aiocb, NUMBER_OF_BYTES);

    state.PauseTiming();

    const auto sum = std::accumulate(write_data.begin(), write_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed. Got: " + std::to_string(sum) + "Expected: " + std::to_string(control_sum));

    state.ResumeTiming();
  }
}

BENCHMARK_DEFINE_F(FileAsyncIOMicroWriteBenchmarkFixture, AIO_WRITE_RANDOM)(benchmark::State& state) {  // open file
  int32_t fd;
  if ((fd = open("file.txt", O_RDONLY)) < 0) {
    std::cout << "open error " << errno << std::endl;
  }

  const auto NUMBER_OF_BYTES = static_cast<uint32_t>(state.range(0) * MB);
  const auto uint32_t_size = ssize_t{sizeof(uint32_t)};
  const auto write_data_size = NUMBER_OF_BYTES / uint32_t_size;
  const auto max_write_data_size = static_cast<size_t>(write_data_size);

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();
    const auto random_indices = generate_random_indexes(vector_element_count);
    auto write_data = std::vector<uint32_t>{};
    write_data.resize(write_data_size);
    auto* write_data_start = std::data(write_data);
    state.ResumeTiming();

    struct aiocb aiocb;

    memset(&aiocb, 0, sizeof(struct aiocb));
    aiocb.aio_fildes = fd;
    aiocb.aio_lio_opcode = LIO_WRITE;
    aiocb.aio_nbytes = uint32_t_size;

    for (auto index = size_t{0}; index < max_write_data_size; ++index) {
      aiocb.aio_offset = uint32_t_size * random_indices[index];
      aiocb.aio_buf = write_data_start + index;
      if (aio_write(&aiocb) == -1) {
        Fail("write error: " + strerror(errno));
      }

      /* Wait until end of transaction */

      std::cout << "Not in progress anymore" << std::endl;
      const auto err = aio_error(&aiocb);
      while (err == EINPROGRESS);

      aio_write_error_handling(&aiocb, NUMBER_OF_BYTES);
    }

    state.PauseTiming();
    const auto sum = std::accumulate(write_data.begin(), write_data.end(), uint64_t{0});
    Assert(control_sum == sum, "Sanity check failed. Got: " + std::to_string(sum) + "Expected: " + std::to_string(control_sum));
    state.ResumeTiming();
  }
}

BENCHMARK_REGISTER_F(FileAsyncIOMicroWriteBenchmarkFixture, AIO_WRITE_SEQUENTIAL)->Arg(10)->Arg(100);
// BENCHMARK_REGISTER_F(FileAsyncIOMicroWriteBenchmarkFixture, AIO_WRITE_RANDOM)->Arg(10)->Arg(100);

}  // namespace hyrise
