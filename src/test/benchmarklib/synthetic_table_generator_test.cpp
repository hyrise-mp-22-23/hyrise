#include "base_test.hpp"
#include "lib/storage/encoding_test.hpp"

#include "hyrise.hpp"
#include "storage/encoding_type.hpp"
#include "storage/table.hpp"
#include "synthetic_table_generator.hpp"

namespace hyrise {

class SyntheticTableGeneratorTest : public BaseTest {};

TEST_F(SyntheticTableGeneratorTest, StringGeneration) {
  EXPECT_EQ(SyntheticTableGenerator::generate_value<pmr_string>(0), "          ");
  EXPECT_EQ(SyntheticTableGenerator::generate_value<pmr_string>(1), "         1");
  EXPECT_EQ(SyntheticTableGenerator::generate_value<pmr_string>(2), "         2");
  EXPECT_EQ(SyntheticTableGenerator::generate_value<pmr_string>(17), "         H");
  EXPECT_EQ(SyntheticTableGenerator::generate_value<pmr_string>(117), "        1t");
  EXPECT_EQ(SyntheticTableGenerator::generate_value<pmr_string>(50'018), "       D0k");
  EXPECT_EQ(SyntheticTableGenerator::generate_value<pmr_string>(3'433'820), "      EPIC");

  // Negative values are not supported.
  ASSERT_THROW(SyntheticTableGenerator::generate_value<pmr_string>(-17), std::logic_error);
}

TEST_F(SyntheticTableGeneratorTest, TestGeneratedValueRange) {
  constexpr auto row_count = size_t{100};
  constexpr auto chunk_size = ChunkOffset{10};
  auto table_generator = std::make_shared<SyntheticTableGenerator>();
  auto uniform_distribution_0_1 = ColumnDataDistribution::make_uniform_config(0.0, 1.0);

  auto table = table_generator->generate_table(
      {{uniform_distribution_0_1, DataType::Double, SegmentEncodingSpec{EncodingType::Dictionary}}}, row_count,
      chunk_size);
  for (auto table_row_id = size_t{0}; table_row_id < 100; ++table_row_id) {
    const auto value = table->get_value<double>(ColumnID{0}, table_row_id);
    ASSERT_TRUE(value >= 0.0 && value <= 1.0);
  }

  EXPECT_EQ(table->row_count(), row_count);
  EXPECT_EQ(table->chunk_count(), row_count / chunk_size);
}

using Params = std::tuple<DataType, ColumnDataDistribution>;

class SyntheticTableGeneratorDataTypeTests : public testing::TestWithParam<Params> {};

TEST_P(SyntheticTableGeneratorDataTypeTests, IntegerTable) {
  constexpr auto row_count = size_t{25};
  constexpr auto chunk_size = ChunkOffset{10};

  const auto tested_data_type = std::get<0>(GetParam());
  auto table_generator = std::make_shared<SyntheticTableGenerator>();

  std::vector<SegmentEncodingSpec> supported_segment_encodings;
  auto replace_unsupporting_encoding_types = [&](SegmentEncodingSpec spec) {
    if (encoding_supports_data_type(spec.encoding_type, tested_data_type)) {
      return spec;
    }
    return SegmentEncodingSpec{EncodingType::Unencoded};
  };
  std::transform(std::begin(all_segment_encoding_specs), std::end(all_segment_encoding_specs),
                 std::back_inserter(supported_segment_encodings), replace_unsupporting_encoding_types);

  std::vector<ColumnSpecification> column_specifications;
  column_specifications.reserve(supported_segment_encodings.size());
  for (auto supported_segment_encoding : supported_segment_encodings) {
    ColumnSpecification column_specification = {std::get<1>(GetParam()), tested_data_type, supported_segment_encoding,
                                                "column_name"};
    column_specifications.push_back(column_specification);
  }

  auto table = table_generator->generate_table(column_specifications, row_count, chunk_size);

  const auto generated_chunk_count = table->chunk_count();
  const auto generated_column_count = table->column_count();
  EXPECT_EQ(table->row_count(), row_count);
  EXPECT_EQ(generated_chunk_count,
            static_cast<size_t>(std::round(static_cast<float>(row_count) / static_cast<float>(chunk_size))));
  EXPECT_EQ(generated_column_count, supported_segment_encodings.size());

  for (auto column_id = ColumnID{0}; column_id < generated_column_count; ++column_id) {
    EXPECT_EQ(table->column_data_type(column_id), tested_data_type);
    EXPECT_EQ(table->column_name(column_id), "column_name");
  }

  for (auto chunk_id = ChunkID{0}; chunk_id < generated_chunk_count; ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    assert_chunk_encoding(chunk, supported_segment_encodings);
  }
}

auto formatter = [](const testing::TestParamInfo<Params> info) {
  auto stream = std::stringstream{};
  switch (std::get<1>(info.param).distribution_type) {
    case DataDistributionType::Uniform:
      stream << "Uniform";
      break;
    case DataDistributionType::Pareto:
      stream << "Pareto";
      break;
    case DataDistributionType::NormalSkewed:
      stream << "Skewed";
  }

  stream << "_" << data_type_to_string.left.at(std::get<0>(info.param));
  return stream.str();
};

// For the skewed distribution, we use a location of 1,000 to move the distribution far into the positive number range.
// The reason is that string values cannot be generated for negative values.
INSTANTIATE_TEST_SUITE_P(SyntheticTableGeneratorDataType, SyntheticTableGeneratorDataTypeTests,
                         testing::Combine(testing::Values(DataType::Int, DataType::Long, DataType::Float,
                                                          DataType::Double, DataType::String),
                                          testing::Values(ColumnDataDistribution::make_uniform_config(0.0, 10'000),
                                                          ColumnDataDistribution::make_pareto_config(),
                                                          ColumnDataDistribution::make_skewed_normal_config(1'000.0))),
                         formatter);
}  // namespace hyrise
