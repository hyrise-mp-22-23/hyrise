#include "jcch_table_generator.hpp"

#include <filesystem>
#include <utility>

#include "utils/timer.hpp"

namespace hyrise {

JCCHTableGenerator::JCCHTableGenerator(const std::string& dbgen_path, const std::string& data_path, float scale_factor,
                                       ClusteringConfiguration clustering_configuration, ChunkOffset chunk_size)
    : JCCHTableGenerator(dbgen_path, data_path, scale_factor, clustering_configuration,
                         create_benchmark_config_with_chunk_size(chunk_size)) {}

JCCHTableGenerator::JCCHTableGenerator(const std::string& dbgen_path, const std::string& data_path, float scale_factor,
                                       ClusteringConfiguration clustering_configuration,
                                       const std::shared_ptr<BenchmarkConfig>& benchmark_config)
    : AbstractTableGenerator(benchmark_config),
      TPCHTableGenerator(scale_factor, clustering_configuration, benchmark_config),
      FileBasedTableGenerator(benchmark_config, data_path),
      _dbgen_path(dbgen_path) {}

std::unordered_map<std::string, BenchmarkTableInfo> JCCHTableGenerator::generate() {
  const auto tables_path = _path + "/tables/";

  // NOLINTBEGIN(concurrency-mt-unsafe)
  // clang-tidy complains that system() is not thread-safe. We ignore this warning as we expect that users will not call
  // the JCCH table generator in parallel.

  // Check if table data has already been generated (and converted to .bin by the FileBasedTableGenerator)
  if (!std::filesystem::exists(tables_path + "/customer.bin")) {
    Timer timer;
    std::cout << "- Creating table data by calling external dbgen" << std::flush;

    std::filesystem::create_directory(tables_path);
    Assert(std::filesystem::exists(tables_path), "Creating JCC-H tables folder failed");

    {
      // Call JCC-H's dbgen
      auto cmd = std::stringstream{};
      // `2>` in a string seems to break Sublime Text's formatter, so it's split into two strings
      cmd << "cd " << tables_path << " && " << _dbgen_path << "/dbgen -f -k -s " << _scale_factor << " -b "
          << _dbgen_path << "/dists.dss >/dev/null 2"
          << ">/dev/null";
      auto ret = system(cmd.str().c_str());
      Assert(!ret, "Calling dbgen failed");
    }

    for (const auto& [_, table_name] : tpch_table_names) {
      // Rename tbl files generated by dbgen to csv so that the correct importer is used
      std::filesystem::rename(tables_path + table_name + ".tbl", tables_path + table_name + ".csv");

      // Remove the trailing separator from each line as the CSVReader does not like them
      {
        // sed on Mac requires a space between -i and '', on Linux it doesn't like it...
#ifdef __APPLE__
        const auto* const sed_inplace = "-i ''";
#else
        const auto* const sed_inplace = "-i''";
#endif

        auto cmd = std::stringstream{};
        cmd << "sed -Ee 's/\\|$//' " << sed_inplace << " " << tables_path << table_name << ".csv";
        auto ret = system(cmd.str().c_str());
        Assert(!ret, "Removing trailing separators using sed failed");
      }

      // std::filesystem::copy does not seem to work. We could use symlinks here, but those would make reading the file
      // via ifstream more complicated.
      {
        auto cmd = std::stringstream{};
        cmd << "cp resources/benchmark/jcch/" << table_name << ".csv.json " << tables_path << table_name << ".csv.json";
        auto ret = system(cmd.str().c_str());
        Assert(!ret, "Copying csv.json files failed");
      }
    }

    std::cout << " (" << timer.lap_formatted() << ")" << std::endl;
  }

  // Having generated the .csv files, call the FileBasedTableGenerator just as if those files were user-provided
  auto generated_tables = FileBasedTableGenerator::generate();

  // FileBasedTableGenerator automatically stores a binary file. Remove the CSV data to save some space.
  if (std::filesystem::exists(tables_path + "/customer.csv")) {
    auto cmd = std::stringstream{};
    cmd << "rm " << tables_path << "*.csv*";
    auto ret = system(cmd.str().c_str());
    Assert(!ret, "Removing csv/csv.json files failed");
  }

  // NOLINTEND(concurrency-mt-unsafe)

  return generated_tables;
}

void JCCHTableGenerator::_add_constraints(
    std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const {
  TPCHTableGenerator::_add_constraints(table_info_by_name);
}

}  // namespace hyrise
