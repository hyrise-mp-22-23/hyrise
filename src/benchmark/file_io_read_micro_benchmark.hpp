#include <fcntl.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <span>

#include "micro_benchmark_basic_fixture.hpp"

namespace hyrise {

class FileIOMicroReadBenchmarkFixture : public MicroBenchmarkBasicFixture {
 public:

    void create_large_file(std::string original_file_name, std::string copied_file_name, uint32_t scale_factor){
        std::ifstream source(original_file_name, std::ios::binary);
        std::ofstream destination(copied_file_name, std::ios::binary);

        if (source.is_open() && destination.is_open()) {
            // Create a buffer to hold the contents of the original file
            std::vector<char> buffer(std::istreambuf_iterator<char>(source), {});

            // Write the contents of the buffer to the destination file
            destination.write(buffer.data(), buffer.size());

            // Append the contents of the buffer 9 times
            for (auto index = uint32_t{0}; index < scale_factor-1; ++index) {
                destination.write(buffer.data(), buffer.size());
            }

            source.close();
            destination.close();
        } else {
            std::cout << "Could not open one of the files" << std::endl;
        }
    }

  void SetUp(::benchmark::State& state) override {
    const auto size_parameter = state.range(0);
    NUMBER_OF_BYTES = _align_to_pagesize(size_parameter);
    NUMBER_OF_ELEMENTS = NUMBER_OF_BYTES / uint32_t_size;
    filename = "benchmark_data_" + std::to_string(size_parameter) + ".txt";

    auto fd = int32_t{};
    if (!std::filesystem::exists(filename)) {
      if(NUMBER_OF_ELEMENTS > MAX_NUMBER_OF_ELEMENTS){
          auto original_file = "benchmark_data_1000.txt";
          create_large_file(original_file, filename, static_cast<uint32_t>(size_parameter/1000));
          chmod(filename.c_str(), S_IRWXU);  // enables owner to rwx file
      }else{
          numbers = generate_random_positive_numbers(NUMBER_OF_ELEMENTS);
          Assert(((fd = creat(filename.c_str(), O_WRONLY)) >= 1),
                 close_file_and_return_error_message(fd, "Create error: ", errno));
          chmod(filename.c_str(), S_IRWXU);  // enables owner to rwx file
          Assert((static_cast<uint64_t>(write(fd, std::data(numbers), NUMBER_OF_BYTES)) == NUMBER_OF_BYTES),
                 close_file_and_return_error_message(fd, "Write error: ", errno));
      }
    }
  Assert(((fd = open(filename.c_str(), O_RDONLY)) >= 0),
         close_file_and_return_error_message(fd, "Open error: ", errno));
  const auto* map = reinterpret_cast<uint32_t*>(mmap(NULL, NUMBER_OF_BYTES, PROT_READ, MAP_PRIVATE, fd, 0));
  Assert((map != MAP_FAILED), close_file_and_return_error_message(fd, "Mapping Failed: ", errno));
  const auto map_span_view = std::span{map, NUMBER_OF_ELEMENTS};
  control_sum = std::accumulate(map_span_view.begin(), map_span_view.end(), uint64_t{0});

  close(fd);
  }

 protected:
  const ssize_t uint32_t_size = ssize_t{sizeof(uint32_t)};
  // MAX_NUMBER_OF_ELEMENTS = NUMBER_OF_ELEMENTS of parameter 1000.
  const uint64_t MAX_NUMBER_OF_ELEMENTS = uint64_t{250000384};
  std::string filename;
  uint64_t control_sum = uint64_t{0};
  uint64_t NUMBER_OF_BYTES = uint64_t{0};
  uint64_t NUMBER_OF_ELEMENTS = uint64_t{0};
  std::vector<uint32_t> numbers = std::vector<uint32_t>{};
  void read_non_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void read_non_atomic_single_threaded(benchmark::State& state);
  void read_non_atomic_random_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void read_non_atomic_random_single_threaded(benchmark::State& state);
  void pread_atomic_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void pread_atomic_single_threaded(benchmark::State& state);
  void pread_atomic_random_multi_threaded(benchmark::State& state, uint16_t thread_count);
  void pread_atomic_random_single_threaded(benchmark::State& state);
#ifdef __linux__
  void libaio_sequential_read_single_threaded(benchmark::State& state);
  void libaio_sequential_read_multi_threaded(benchmark::State& state, uint16_t aio_request_count);
  void libaio_random_read(benchmark::State& state, uint16_t aio_request_count);
#endif
  void memory_mapped_read_single_threaded(benchmark::State& state, const int mapping_type, const int map_mode_flag, const int access_order);
  #ifdef __linux__
  void memory_mapped_read_user_space(benchmark::State& state, const uint16_t thread_count, const int access_order);
  #endif
  void memory_mapped_read_multi_threaded(benchmark::State& state, const int mapping_type, const int map_mode_flag, const uint16_t thread_count, const int access_order);


  // enums for mmap benchmarks
  enum MAPPING_TYPE { MMAP, UMAP };

  enum DATA_ACCESS_TYPES { SEQUENTIAL, RANDOM };

  enum MAP_ACCESS_TYPES { SHARED = MAP_SHARED, PRIVATE = MAP_PRIVATE };
};
}  // namespace hyrise
