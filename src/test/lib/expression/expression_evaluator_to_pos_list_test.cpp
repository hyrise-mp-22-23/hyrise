#include <optional>

#include "base_test.hpp"

#include "expression/arithmetic_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/exists_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "expression/value_expression.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

class ExpressionEvaluatorToPosListTest : public BaseTest {
 public:
  void SetUp() override {
    table_a = load_table("resources/test_data/tbl/expression_evaluator/input_a.tbl", ChunkOffset{4});
    table_b = load_table("resources/test_data/tbl/expression_evaluator/input_b.tbl", ChunkOffset{4});
    c = PQPColumnExpression::from_table(*table_a, "c");
    d = PQPColumnExpression::from_table(*table_a, "d");
    s1 = PQPColumnExpression::from_table(*table_a, "s1");
    s3 = PQPColumnExpression::from_table(*table_a, "s3");
    x = PQPColumnExpression::from_table(*table_b, "x");
  }

  bool test_expression(const std::shared_ptr<Table>& table, const ChunkID chunk_id,
                       const AbstractExpression& expression, const std::vector<ChunkOffset>& matching_chunk_offsets) {
    const auto actual_pos_list = ExpressionEvaluator{table, chunk_id}.evaluate_expression_to_pos_list(expression);

    auto expected_pos_list = RowIDPosList{};
    expected_pos_list.resize(matching_chunk_offsets.size());
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < matching_chunk_offsets.size(); ++chunk_offset) {
      expected_pos_list[chunk_offset] = RowID{chunk_id, matching_chunk_offsets[chunk_offset]};
    }

    return actual_pos_list == expected_pos_list;
  }

  std::shared_ptr<Table> table_a, table_b;

  std::shared_ptr<PQPColumnExpression> c, d, s1, s3, x;
};

TEST_F(ExpressionEvaluatorToPosListTest, PredicateWithoutNulls) {
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *less_than_(x, 9), {ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *less_than_(x, 8), {ChunkOffset{1}}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *less_than_equals_(x, 9), {ChunkOffset{1}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *less_than_equals_(x, 7), {ChunkOffset{1}}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *equals_(x, 10), {ChunkOffset{0}, ChunkOffset{2}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *equals_(x, 8), {ChunkOffset{0}, ChunkOffset{2}}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *not_equals_(x, 10), {ChunkOffset{1}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *not_equals_(x, 8), {ChunkOffset{1}}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *greater_than_(x, 9), {ChunkOffset{0}, ChunkOffset{2}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *greater_than_(x, 9), {}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *greater_than_equals_(x, 9),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *greater_than_equals_(x, 8), {ChunkOffset{0}, ChunkOffset{2}}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_inclusive_(x, 8, 9), {ChunkOffset{1}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *between_inclusive_(x, 7, 8),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_lower_exclusive_(x, 8, 9), {ChunkOffset{1}}));
  EXPECT_TRUE(
      test_expression(table_b, ChunkID{1}, *between_lower_exclusive_(x, 7, 8), {ChunkOffset{0}, ChunkOffset{2}}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_upper_exclusive_(x, 8, 9), {ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *between_upper_exclusive_(x, 7, 8), {ChunkOffset{1}}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_exclusive_(x, 8, 9), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *between_exclusive_(x, 7, 8), {}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *in_(x, list_(9, "hello", 10)),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *in_(x, list_(1, 2, 7)), {ChunkOffset{1}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *not_in_(x, list_(9, "hello", 10)), {ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *not_in_(x, list_(1, 2, 7)), {ChunkOffset{0}, ChunkOffset{2}}));

  EXPECT_TRUE(
      test_expression(table_a, ChunkID{0}, *like_(s1, "%a%"), {ChunkOffset{0}, ChunkOffset{2}, ChunkOffset{3}}));

  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *not_like_(s1, "%a%"), {ChunkOffset{1}}));
}

TEST_F(ExpressionEvaluatorToPosListTest, PredicatesWithOnlyLiterals) {
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *like_("hello", "%ll%"),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *like_("hello", "%lol%"), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *in_(5, list_(1, 2)), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *in_(5, list_(1, 2, 5)),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *greater_than_(5, 1),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(
      test_expression(table_b, ChunkID{1}, *greater_than_(5, 1), {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *between_inclusive_(2, 5, 6), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_inclusive_(1, 1, 6),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_inclusive_(6, 1, 6),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_inclusive_(2, 1, 6),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *between_lower_exclusive_(2, 5, 6), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_lower_exclusive_(1, 1, 6), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_lower_exclusive_(6, 1, 6),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_lower_exclusive_(2, 1, 6),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *between_upper_exclusive_(2, 5, 6), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_upper_exclusive_(1, 1, 6),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_upper_exclusive_(6, 1, 6), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_upper_exclusive_(2, 1, 6),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *between_exclusive_(2, 5, 6), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_exclusive_(1, 1, 6), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_exclusive_(6, 1, 6), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_exclusive_(2, 1, 6),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *value_(1),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *value_(0), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *is_null_(0), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *is_null_(null_()),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *or_(0, 1),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *or_(0, 0), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *and_(0, 1), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *and_(1, 1),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
}

TEST_F(ExpressionEvaluatorToPosListTest, PredicateWithNulls) {
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *equals_(c, 33), {ChunkOffset{0}}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *not_equals_(c, 33), {ChunkOffset{2}}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *less_than_(c, 35), {ChunkOffset{0}, ChunkOffset{2}}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *less_than_equals_(c, 35), {ChunkOffset{0}, ChunkOffset{2}}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *greater_than_(c, 33), {ChunkOffset{2}}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *greater_than_equals_(c, 0), {ChunkOffset{0}, ChunkOffset{2}}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *between_inclusive_(c, 33, 34), {ChunkOffset{0}, ChunkOffset{2}}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *between_lower_exclusive_(c, 33, 34), {ChunkOffset{2}}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *between_upper_exclusive_(c, 33, 34), {ChunkOffset{0}}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *between_exclusive_(c, 33, 34), {}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *between_exclusive_(c, 33, 35), {ChunkOffset{2}}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *is_null_(c), {ChunkOffset{1}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *is_not_null_(c), {ChunkOffset{0}, ChunkOffset{2}}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *in_(c, list_(0, null_(), 33)), {ChunkOffset{0}}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *in_(c, list_(0, null_(), 33)), {ChunkOffset{0}}));
}

TEST_F(ExpressionEvaluatorToPosListTest, LogicalWithoutNulls) {
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *and_(greater_than_equals_(x, 8), less_than_(x, 10)),
                              {ChunkOffset{1}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *and_(less_than_(x, 9), less_than_(x, 8)), {ChunkOffset{1}}));

  EXPECT_TRUE(
      test_expression(table_b, ChunkID{0}, *or_(equals_(x, 10), less_than_(x, 2)), {ChunkOffset{0}, ChunkOffset{2}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *or_(equals_(x, 10), not_equals_(x, 8)),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}}));
}

TEST_F(ExpressionEvaluatorToPosListTest, LogicalWithNulls) {
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *and_(is_not_null_(c), equals_(c, 33)), {ChunkOffset{0}}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *or_(is_null_(c), equals_(c, 33)),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{3}}));
}

TEST_F(ExpressionEvaluatorToPosListTest, ExistsCorrelated) {
  const auto table_wrapper = std::make_shared<TableWrapper>(table_a);
  table_wrapper->never_clear_output();
  const auto table_scan =
      std::make_shared<TableScan>(table_wrapper, equals_(d, correlated_parameter_(ParameterID{0}, x)));
  const auto subquery = pqp_subquery_(table_scan, DataType::Int, false, std::make_pair(ParameterID{0}, ColumnID{0}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *exists_(subquery), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *exists_(subquery), {ChunkOffset{1}}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *not_exists_(subquery),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *not_exists_(subquery), {ChunkOffset{0}, ChunkOffset{2}}));
}

TEST_F(ExpressionEvaluatorToPosListTest, ExistsUncorrelated) {
  const auto table_wrapper_all = std::make_shared<TableWrapper>(Projection::dummy_table());
  table_wrapper_all->never_clear_output();
  const auto subquery_returning_all = pqp_subquery_(table_wrapper_all, DataType::Int, false);

  const auto empty_table =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data);
  const auto table_wrapper_empty = std::make_shared<TableWrapper>(empty_table);
  const auto subquery_returning_none = pqp_subquery_(table_wrapper_empty, DataType::Int, false);

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *exists_(subquery_returning_all),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *exists_(subquery_returning_none), {}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *not_exists_(subquery_returning_all), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *not_exists_(subquery_returning_none),
                              {ChunkOffset{0}, ChunkOffset{1}, ChunkOffset{2}, ChunkOffset{3}}));
}

}  // namespace hyrise
