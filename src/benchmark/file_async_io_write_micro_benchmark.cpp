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

const auto MEGA = uint32_t{1'000'000};

class FileAsyncIOMicroWriteBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  uint64_t control_sum = uint64_t{0};
  std::vector<uint32_t> numbers = std::vector<uint32_t>{};
  uint32_t vector_element_count;

  void SetUp(::benchmark::State& state) override {
    // TODO(everybody): Make setup/teardown global per file size to improve benchmark speed
    ssize_t BUFFER_SIZE_MB = state.range(0);

    // each int32_t contains four bytes
    vector_element_count = (BUFFER_SIZE_MB * MEGA) / sizeof(uint32_t);
    numbers = std::vector<uint32_t>(vector_element_count);
    for (auto index = size_t{0}; index < vector_element_count; ++index) {
      numbers[index] = std::rand() % UINT32_MAX;
    }
    control_sum = std::accumulate(numbers.begin(), numbers.end(), uint64_t{0});

    int32_t fd;
    Assert((fd = creat("file.txt", O_WRONLY)) !=  -1, "Create error: " + strerror(errno));

    chmod("file.txt", S_IRWXU);  // enables owner to rwx file
    Assert(write(fd, std::data(numbers), BUFFER_SIZE_MB * MEGA) == BUFFER_SIZE_MB * MEGA, "Write error: " + strerror(errno));

    close(fd);
  }

  void aio_write_error_handling(aiocb* aiocb, uint32_t EXPECTED_BYTES){
    const auto err = aio_error(aiocb);
    const auto ret = aio_return(aiocb);

    Assert(err == 0, "Error at aio_error(): " + strerror(errno));

    Assert(ret == static_cast<int32_t>(EXPECTED_BYTES), "Error at aio_return(). Got: " + std::to_string(ret) +
      " Expected: " + std::to_string(EXPECTED_BYTES) + ".");
  }

  void TearDown(::benchmark::State& /*state*/) override {
    // TODO(everybody): Error handling
    std::remove("file.txt");
  }
};

BENCHMARK_DEFINE_F(FileAsyncIOMicroWriteBenchmarkFixture, AIO_WRITE_SEQUENTIAL)(benchmark::State& state) {  // open file
  int32_t fd;
  Assert((fd = open("file.txt", O_WRONLY)) != -1, "Open error: " + strerror(errno));

  const auto NUMBER_OF_BYTES = static_cast<uint32_t>(state.range(0) * MEGA);

  for (auto _ : state) {
    state.PauseTiming();

    micro_benchmark_clear_disk_cache();

    state.ResumeTiming();

    // The standard aio control block. A structure, that holds the information about the asnyc IO op.
    struct aiocb aiocb;
    memset(&aiocb, 0, sizeof(struct aiocb));
    aiocb.aio_fildes = fd;
    aiocb.aio_buf = std::data(numbers);
    aiocb.aio_nbytes = NUMBER_OF_BYTES;
    aiocb.aio_lio_opcode = LIO_WRITE;

    Assert(aio_write(&aiocb) != -1, "Write error: " + strerror(errno));
    
    auto err = aio_error(&aiocb);
    /* Wait until end of transaction */
    while (err == EINPROGRESS) {
      err = aio_error(&aiocb);
    }

    aio_write_error_handling(&aiocb, NUMBER_OF_BYTES);
  }
}

BENCHMARK_REGISTER_F(FileAsyncIOMicroWriteBenchmarkFixture, AIO_WRITE_SEQUENTIAL)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace hyrise
