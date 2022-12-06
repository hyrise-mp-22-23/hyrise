#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <numeric>

#include "micro_benchmark_basic_fixture.hpp"

namespace hyrise {

const auto MB = uint32_t{1'000'000};

class FileIOMicroReadBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    NUMBER_OF_BYTES = _align_to_pagesize(state.range(0));
    NUMBER_OF_ELEMENTS = NUMBER_OF_BYTES / uint32_t_size;

    // each int32_t contains four bytes
    numbers = generate_random_positive_numbers(NUMBER_OF_ELEMENTS);
    control_sum = std::accumulate(numbers.begin(), numbers.end(), uint64_t{0});

    auto fd = int32_t{};
    Assert(((fd = creat(filename, O_WRONLY)) >= 1), fail_and_close_file(fd, "Create error: ", errno));
    chmod(filename, S_IRWXU);  // enables owner to rwx file
    Assert((write(fd, std::data(numbers), NUMBER_OF_BYTES) == NUMBER_OF_BYTES),
           fail_and_close_file(fd, "Write error: ", errno));
    close(fd);
  }

  void TearDown(::benchmark::State& /*state*/) override {
    Assert(std::remove(filename) == 0, "Remove error: " + std::strerror(errno));
  }

 protected:
  const char* filename = "file.txt";  // const char* needed for C-System Calls
  const ssize_t uint32_t_size = ssize_t{sizeof(uint32_t)};
  uint64_t control_sum = uint64_t{0};
  uint32_t NUMBER_OF_BYTES = uint32_t{0};
  uint32_t NUMBER_OF_ELEMENTS = uint32_t{0};
  std::vector<uint32_t> numbers = std::vector<uint32_t>{};
  void read_non_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void read_non_atomic_single_threaded(benchmark::State& state);
  void read_non_atomic_random_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void read_non_atomic_random_single_threaded(benchmark::State& state);
  void pread_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void pread_atomic_single_threaded(benchmark::State& state);
  void pread_atomic_random_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void pread_atomic_random_single_threaded(benchmark::State& state);
};
}  // namespace hyrise
