#include <filesystem>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "base_test.hpp"

#include "./storage_manager_test_util.cpp"
#include "hyrise.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/table.hpp"
#include "utils/meta_table_manager.hpp"

namespace hyrise {

class StorageManagerTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& sm = Hyrise::get().storage_manager;
    auto t1 = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data);
    auto t2 =
        std::make_shared<Table>(TableColumnDefinitions{{"b", DataType::Int, false}}, TableType::Data, ChunkOffset{4});

    sm.add_table("first_table", t1);
    sm.add_table("second_table", t2);

    const auto v1_lqp = StoredTableNode::make("first_table");
    const auto v1 = std::make_shared<LQPView>(v1_lqp, std::unordered_map<ColumnID, std::string>{});

    const auto v2_lqp = StoredTableNode::make("second_table");
    const auto v2 = std::make_shared<LQPView>(v2_lqp, std::unordered_map<ColumnID, std::string>{});

    sm.add_view("first_view", std::move(v1));
    sm.add_view("second_view", std::move(v2));

    const auto pp1_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
    const auto pp1 = std::make_shared<PreparedPlan>(pp1_lqp, std::vector<ParameterID>{});

    const auto pp2_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Float, "b"}}, "b");
    const auto pp2 = std::make_shared<PreparedPlan>(pp2_lqp, std::vector<ParameterID>{});

    sm.add_prepared_plan("first_prepared_plan", std::move(pp1));
    sm.add_prepared_plan("second_prepared_plan", std::move(pp2));
  }
  const uint32_t file_header_bytes = StorageManager::_file_header_bytes;

};

TEST_F(StorageManagerTest, AddTableTwice) {
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_THROW(sm.add_table("first_table", std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data)),
               std::exception);
  EXPECT_THROW(sm.add_table("first_view", std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data)),
               std::exception);
}

TEST_F(StorageManagerTest, StatisticCreationOnAddTable) {
  auto& sm = Hyrise::get().storage_manager;
  sm.add_table("int_float", load_table("resources/test_data/tbl/int_float.tbl"));

  const auto table = sm.get_table("int_float");
  EXPECT_EQ(table->table_statistics()->row_count, 3.0f);
  const auto chunk = table->get_chunk(ChunkID{0});
  EXPECT_TRUE(chunk->pruning_statistics().has_value());
  EXPECT_EQ(chunk->pruning_statistics()->at(0)->data_type, DataType::Int);
  EXPECT_EQ(chunk->pruning_statistics()->at(1)->data_type, DataType::Float);
}

TEST_F(StorageManagerTest, GetTable) {
  auto& sm = Hyrise::get().storage_manager;
  auto t3 = sm.get_table("first_table");
  auto t4 = sm.get_table("second_table");
  EXPECT_THROW(sm.get_table("third_table"), std::exception);
  auto names = std::vector<std::string>{"first_table", "second_table"};
  auto sm_names = sm.table_names();
  std::sort(sm_names.begin(), sm_names.end());
  EXPECT_EQ(sm_names, names);
}

TEST_F(StorageManagerTest, DropTable) {
  auto& sm = Hyrise::get().storage_manager;
  sm.drop_table("first_table");
  EXPECT_THROW(sm.get_table("first_table"), std::exception);
  EXPECT_THROW(sm.drop_table("first_table"), std::exception);

  const auto& tables = sm.tables();
  EXPECT_EQ(tables.size(), 1);

  sm.add_table("first_table", std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data));
  EXPECT_TRUE(sm.has_table("first_table"));
}

TEST_F(StorageManagerTest, DoesNotHaveTable) {
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_EQ(sm.has_table("third_table"), false);
}

TEST_F(StorageManagerTest, HasTable) {
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_EQ(sm.has_table("first_table"), true);
}

TEST_F(StorageManagerTest, AddViewTwice) {
  const auto v1_lqp = StoredTableNode::make("first_table");
  const auto v1 = std::make_shared<LQPView>(v1_lqp, std::unordered_map<ColumnID, std::string>{});

  auto& sm = Hyrise::get().storage_manager;
  EXPECT_THROW(sm.add_view("first_table", v1), std::exception);
  EXPECT_THROW(sm.add_view("first_view", v1), std::exception);
}

TEST_F(StorageManagerTest, GetView) {
  auto& sm = Hyrise::get().storage_manager;
  auto v3 = sm.get_view("first_view");
  auto v4 = sm.get_view("second_view");
  EXPECT_THROW(sm.get_view("third_view"), std::exception);
}

TEST_F(StorageManagerTest, DropView) {
  auto& sm = Hyrise::get().storage_manager;
  sm.drop_view("first_view");
  EXPECT_THROW(sm.get_view("first_view"), std::exception);
  EXPECT_THROW(sm.drop_view("first_view"), std::exception);

  const auto& views = sm.views();
  EXPECT_EQ(views.size(), 1);

  const auto v1_lqp = StoredTableNode::make("first_table");
  const auto v1 = std::make_shared<LQPView>(v1_lqp, std::unordered_map<ColumnID, std::string>{});
  sm.add_view("first_view", v1);
  EXPECT_TRUE(sm.has_view("first_view"));
}

TEST_F(StorageManagerTest, ResetView) {
  Hyrise::reset();
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_THROW(sm.get_view("first_view"), std::exception);
}

TEST_F(StorageManagerTest, DoesNotHaveView) {
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_EQ(sm.has_view("third_view"), false);
}

TEST_F(StorageManagerTest, HasView) {
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_EQ(sm.has_view("first_view"), true);
}

TEST_F(StorageManagerTest, ListViewNames) {
  auto& sm = Hyrise::get().storage_manager;
  const auto view_names = sm.view_names();

  EXPECT_EQ(view_names.size(), 2u);

  EXPECT_EQ(view_names[0], "first_view");
  EXPECT_EQ(view_names[1], "second_view");
}

TEST_F(StorageManagerTest, OutputToStream) {
  auto& sm = Hyrise::get().storage_manager;
  sm.add_table("third_table", load_table("resources/test_data/tbl/int_int2.tbl", ChunkOffset{2}));

  std::ostringstream output;
  output << sm;
  auto output_string = output.str();

  EXPECT_TRUE(output_string.find("===== Tables =====") != std::string::npos);
  EXPECT_TRUE(output_string.find("==== table >> first_table << (1 columns, 0 rows in 0 chunks)") != std::string::npos);
  EXPECT_TRUE(output_string.find("==== table >> second_table << (1 columns, 0 rows in 0 chunks)") != std::string::npos);
  EXPECT_TRUE(output_string.find("==== table >> third_table << (2 columns, 4 rows in 2 chunks)") != std::string::npos);

  EXPECT_TRUE(output_string.find("===== Views ======") != std::string::npos);
  EXPECT_TRUE(output_string.find("==== view >> first_view <<") != std::string::npos);
  EXPECT_TRUE(output_string.find("==== view >> second_view <<") != std::string::npos);
}

TEST_F(StorageManagerTest, ExportTables) {
  std::ostringstream output;
  auto& sm = Hyrise::get().storage_manager;

  // first, we remove empty test tables
  sm.drop_table("first_table");
  sm.drop_table("second_table");

  // add a non-empty table
  sm.add_table("third_table", load_table("resources/test_data/tbl/int_float.tbl"));

  sm.export_all_tables_as_csv(test_data_path);

  const std::string filename = test_data_path + "/third_table.csv";
  EXPECT_TRUE(std::filesystem::exists(filename));
  std::filesystem::remove(filename);
}

TEST_F(StorageManagerTest, AddPreparedPlanTwice) {
  auto& sm = Hyrise::get().storage_manager;

  const auto pp1_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
  const auto pp1 = std::make_shared<PreparedPlan>(pp1_lqp, std::vector<ParameterID>{});

  EXPECT_THROW(sm.add_prepared_plan("first_prepared_plan", pp1), std::exception);
}

TEST_F(StorageManagerTest, GetPreparedPlan) {
  auto& sm = Hyrise::get().storage_manager;
  auto pp3 = sm.get_prepared_plan("first_prepared_plan");
  auto pp4 = sm.get_prepared_plan("second_prepared_plan");
  EXPECT_THROW(sm.get_prepared_plan("third_prepared_plan"), std::exception);
}

TEST_F(StorageManagerTest, DropPreparedPlan) {
  auto& sm = Hyrise::get().storage_manager;
  sm.drop_prepared_plan("first_prepared_plan");
  EXPECT_THROW(sm.get_prepared_plan("first_prepared_plan"), std::exception);
  EXPECT_THROW(sm.drop_prepared_plan("first_prepared_plan"), std::exception);

  const auto& prepared_plans = sm.prepared_plans();
  EXPECT_EQ(prepared_plans.size(), 1);

  const auto pp_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
  const auto pp = std::make_shared<PreparedPlan>(pp_lqp, std::vector<ParameterID>{});

  sm.add_prepared_plan("first_prepared_plan", pp);
  EXPECT_TRUE(sm.has_prepared_plan("first_prepared_plan"));
}

TEST_F(StorageManagerTest, DoesNotHavePreparedPlan) {
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_EQ(sm.has_prepared_plan("third_prepared_plan"), false);
}

TEST_F(StorageManagerTest, HasPreparedPlan) {
  auto& sm = Hyrise::get().storage_manager;
  EXPECT_EQ(sm.has_prepared_plan("first_prepared_plan"), true);
}

TEST_F(StorageManagerTest, PersistencyOffsetEnds) {
  auto& sm = Hyrise::get().storage_manager;

  const auto COLUMN_COUNT = uint32_t{5};
  const auto ROW_COUNT = uint32_t{500};

  const auto segment_offsets = sm.generate_segment_offset_ends(
    StorageManagerTestUtil::create_dictionary_segment_chunk(ROW_COUNT, COLUMN_COUNT)
  );

  const auto expected_offsets = std::vector<uint32_t>{ 3036, 4548, 5728, 6740, 7652 };

  EXPECT_EQ(expected_offsets.size(), segment_offsets.size());
  EXPECT_EQ(expected_offsets, segment_offsets);
}

TEST_F(StorageManagerTest, PersistencyDifferentChunks) {
  auto& sm = Hyrise::get().storage_manager;

  const auto file_name = "test_various_chunks.bin";
  const auto MAX_CHUNK_COUNT = sm.get_max_chunk_count_per_file();
  
  std::remove(file_name);

  const auto small_chunk1 = StorageManagerTestUtil::create_dictionary_segment_chunk(100, 5);
  const auto small_chunk2 = StorageManagerTestUtil::create_dictionary_segment_chunk(123, 1);
  const auto small_chunk3 = StorageManagerTestUtil::create_dictionary_segment_chunk(40, 10);
  const auto small_chunk4 = StorageManagerTestUtil::create_dictionary_segment_chunk(5, 10);
  const auto small_chunk5 = StorageManagerTestUtil::create_dictionary_segment_chunk(400, 30);
  const auto small_chunk6 = StorageManagerTestUtil::create_dictionary_segment_chunk(77, 3);
  const auto small_chunk7 = StorageManagerTestUtil::create_dictionary_segment_chunk(20, 9);

  const auto medium_chunk1 = StorageManagerTestUtil::create_dictionary_segment_chunk(3'000, 50);
  const auto medium_chunk2 = StorageManagerTestUtil::create_dictionary_segment_chunk(4'500, 70);
  const auto medium_chunk3 = StorageManagerTestUtil::create_dictionary_segment_chunk(2'000, 25);
  const auto medium_chunk4 = StorageManagerTestUtil::create_dictionary_segment_chunk(1'500, 55);
  const auto medium_chunk5 = StorageManagerTestUtil::create_dictionary_segment_chunk(4'500, 20);

  const auto big_chunk1 = StorageManagerTestUtil::create_dictionary_segment_chunk(50'000, 500);
  const auto big_chunk2 = StorageManagerTestUtil::create_dictionary_segment_chunk(100'000, 30);
  const auto big_chunk3 = StorageManagerTestUtil::create_dictionary_segment_chunk(90'000, 400);
  const auto big_chunk4 = StorageManagerTestUtil::create_dictionary_segment_chunk(80'000, 200);

  const std::vector<std::shared_ptr<Chunk>> chunks {
    small_chunk1, small_chunk2, small_chunk3, small_chunk4, small_chunk5, small_chunk6, small_chunk7,
    medium_chunk1, medium_chunk2, medium_chunk3, medium_chunk4, medium_chunk5,
    big_chunk1, big_chunk2, big_chunk3, big_chunk4
  };

  sm.persist_chunks_to_disk(chunks, file_name);
  EXPECT_TRUE(std::filesystem::exists(file_name));

  const auto read_header = sm.read_file_header(file_name);
  EXPECT_EQ(read_header.chunk_count, chunks.size());
  EXPECT_EQ(read_header.storage_format_version_id, sm.get_storage_format_version_id());
  EXPECT_EQ(read_header.chunk_ids.size(), MAX_CHUNK_COUNT);
  EXPECT_EQ(read_header.chunk_offset_ends.size(), MAX_CHUNK_COUNT);  

  // Equivalent to medium_chunk1.
  const auto mapped_chunks = sm.map_chunk_from_disk(read_header.chunk_offset_ends[6], file_name, 50);

  const auto segment_index = uint16_t{4};

  const auto expected_sum = StorageManagerTestUtil::accumulate_sum_of_segment(medium_chunk1, segment_index);
  const auto mapped_sum = StorageManagerTestUtil::accumulate_sum_of_segment(mapped_chunks, segment_index);

  EXPECT_EQ(expected_sum, mapped_sum);
}

TEST_F(StorageManagerTest, PersistencyWriteEmptyListOfChunks) {
  auto& sm = Hyrise::get().storage_manager;
  const auto file_name = "test_empty_list_of_chunks.bin";
  std::remove(file_name);

  std::vector<std::shared_ptr<Chunk>> chunks(0);
  const auto CHUNK_COUNT = sm.get_max_chunk_count_per_file();

  sm.persist_chunks_to_disk(chunks, file_name);
  EXPECT_TRUE(std::filesystem::exists(file_name));

  const auto read_header = sm.read_file_header(file_name);
  EXPECT_EQ(read_header.chunk_count, chunks.size());
  EXPECT_EQ(read_header.storage_format_version_id, sm.get_storage_format_version_id());
  EXPECT_EQ(read_header.chunk_ids.size(), CHUNK_COUNT);
  EXPECT_EQ(read_header.chunk_offset_ends.size(), CHUNK_COUNT);
}

TEST_F(StorageManagerTest, PersistencyWriteMaxNumberOfChunksToFile) {
  auto& sm = Hyrise::get().storage_manager;

  const auto file_name = "test_chunks_max_number_of_chunks.bin";
  const auto ROW_COUNT = uint32_t{65000};
  const auto COLUMN_COUNT = uint32_t{23};
  const auto CHUNK_COUNT = sm.get_max_chunk_count_per_file();

  const auto chunks = StorageManagerTestUtil::get_chunks(file_name, ROW_COUNT, COLUMN_COUNT, CHUNK_COUNT);
  sm.persist_chunks_to_disk(chunks, file_name);

  EXPECT_TRUE(std::filesystem::exists(file_name));

  const auto read_header = sm.read_file_header(file_name);

  EXPECT_EQ(read_header.chunk_count, CHUNK_COUNT);
  EXPECT_EQ(read_header.storage_format_version_id, sm.get_storage_format_version_id());
  EXPECT_EQ(read_header.chunk_ids.size(), CHUNK_COUNT);
  EXPECT_EQ(read_header.chunk_offset_ends.size(), CHUNK_COUNT);

  const auto mapped_chunks = StorageManagerTestUtil::map_chunks_from_file(file_name, COLUMN_COUNT, read_header);

  const auto chunk_index = uint16_t{0};
  const auto segment_index = uint16_t{16};
  
  const auto expected_sum = StorageManagerTestUtil::accumulate_sum_of_segment(chunks, chunk_index, segment_index);
  const auto mapped_sum = StorageManagerTestUtil::accumulate_sum_of_segment(mapped_chunks, chunk_index, segment_index);

  EXPECT_EQ(expected_sum, mapped_sum);
}

TEST_F(StorageManagerTest, WriteMaxNumberOfChunksToFileSmall) {
  const auto file_name = "test_chunks_file.bin";
  std::remove(file_name);
  auto& sm = Hyrise::get().storage_manager;

  constexpr auto ROW_COUNT = uint32_t{100};  // can't be greater than INT32_MAX
  constexpr auto COLUMN_COUNT = uint32_t{23};
  auto CHUNK_COUNT = sm.get_max_chunk_count_per_file();

  const auto chunk = StorageManagerTestUtil::create_dictionary_segment_chunk(ROW_COUNT, COLUMN_COUNT);
  std::vector<std::shared_ptr<Chunk>> chunks(CHUNK_COUNT);
  for (auto index = size_t{0}; index < chunks.size(); ++index) {
    chunks[index] = chunk;
  }
  sm.persist_chunks_to_disk(chunks, file_name);

  EXPECT_TRUE(std::filesystem::exists(file_name));

  const auto read_header = sm.read_file_header(file_name);

  EXPECT_EQ(read_header.chunk_count, CHUNK_COUNT);
  EXPECT_EQ(read_header.storage_format_version_id, sm.get_storage_format_version_id());
  EXPECT_EQ(read_header.chunk_ids.size(), CHUNK_COUNT);
  EXPECT_EQ(read_header.chunk_offset_ends.size(), CHUNK_COUNT);

  const auto mapped_chunks = StorageManagerTestUtil::map_chunks_from_file(file_name, COLUMN_COUNT, read_header);

  const auto chunk_index = uint16_t{0};
  const auto segment_index = uint16_t{16};

  const auto expected_sum = StorageManagerTestUtil::accumulate_sum_of_segment(chunks, chunk_index, segment_index);
  const auto mapped_sum = StorageManagerTestUtil::accumulate_sum_of_segment(mapped_chunks, chunk_index, segment_index);

  EXPECT_EQ(expected_sum, mapped_sum);
}

TEST_F(StorageManagerTest, PersistencyWrite32BitNumbersToFile) {
  const auto file_name = "test_chunks_file.bin";
  std::remove(file_name);
  auto& sm = Hyrise::get().storage_manager;

  const auto ROW_COUNT = uint32_t{UINT16_MAX + 1};  // can't be greater than INT32_MAX
  const auto COLUMN_COUNT = uint32_t{23};
  const auto CHUNK_COUNT = sm.get_max_chunk_count_per_file();

  const auto chunk = StorageManagerTestUtil::create_dictionary_segment_chunk(ROW_COUNT, COLUMN_COUNT);
  std::vector<std::shared_ptr<Chunk>> chunks(CHUNK_COUNT);
  for (auto index = size_t{0}; index < chunks.size(); ++index) {
    chunks[index] = chunk;
  }
  sm.persist_chunks_to_disk(chunks, file_name);

  EXPECT_TRUE(std::filesystem::exists(file_name));

  const auto read_header = sm.read_file_header(file_name);

  EXPECT_EQ(read_header.chunk_count, CHUNK_COUNT);
  EXPECT_EQ(read_header.storage_format_version_id, sm.get_storage_format_version_id());
  EXPECT_EQ(read_header.chunk_ids.size(), CHUNK_COUNT);
  EXPECT_EQ(read_header.chunk_offset_ends.size(), CHUNK_COUNT);

  const auto mapped_chunks = StorageManagerTestUtil::map_chunks_from_file(file_name, COLUMN_COUNT, read_header);

  const auto chunk_index = uint16_t{0};
  const auto segment_index = uint16_t{16};

  const auto expected_sum = StorageManagerTestUtil::accumulate_sum_of_segment(chunks, chunk_index, segment_index);
  const auto mapped_sum = StorageManagerTestUtil::accumulate_sum_of_segment(mapped_chunks, chunk_index, segment_index);

  EXPECT_EQ(expected_sum, mapped_sum);
}

}  // namespace hyrise
