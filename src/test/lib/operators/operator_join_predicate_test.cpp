#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "operators/operator_join_predicate.hpp"

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

class OperatorJoinPredicateTest : public BaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");

    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
    b_a = node_b->get_column("a");
    b_b = node_b->get_column("b");
  }

  std::shared_ptr<MockNode> node_a, node_b;
  std::shared_ptr<LQPColumnExpression> a_a, a_b, b_a, b_b;
};

TEST_F(OperatorJoinPredicateTest, FromExpression) {
  const auto predicate_a = OperatorJoinPredicate::from_expression(*equals_(a_a, b_b), *node_a, *node_b);
  ASSERT_TRUE(predicate_a);
  EXPECT_EQ(predicate_a->column_ids.first, ColumnID{0});
  EXPECT_EQ(predicate_a->column_ids.second, ColumnID{1});
  EXPECT_EQ(predicate_a->predicate_condition, PredicateCondition::Equals);
  EXPECT_FALSE(predicate_a->is_flipped());

  const auto predicate_b = OperatorJoinPredicate::from_expression(*less_than_(b_a, a_b), *node_a, *node_b);
  ASSERT_TRUE(predicate_b);
  EXPECT_EQ(predicate_b->column_ids.first, ColumnID{1});
  EXPECT_EQ(predicate_b->column_ids.second, ColumnID{0});
  EXPECT_EQ(predicate_b->predicate_condition, PredicateCondition::GreaterThan);
  EXPECT_TRUE(predicate_b->is_flipped());
}

TEST_F(OperatorJoinPredicateTest, FromExpressionImpossible) {
  const auto predicate_a = OperatorJoinPredicate::from_expression(*equals_(a_a, a_b), *node_a, *node_b);
  ASSERT_FALSE(predicate_a);

  const auto predicate_b = OperatorJoinPredicate::from_expression(*less_than_(add_(b_a, 5), a_b), *node_a, *node_b);
  ASSERT_FALSE(predicate_b);
}

TEST_F(OperatorJoinPredicateTest, Flip) {
  auto predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{1}}, PredicateCondition::LessThanEquals};
  EXPECT_FALSE(predicate.is_flipped());

  predicate.flip();
  EXPECT_TRUE(predicate.is_flipped());
  EXPECT_EQ(predicate.column_ids.first, ColumnID{1});
  EXPECT_EQ(predicate.column_ids.second, ColumnID{0});
  EXPECT_EQ(predicate.predicate_condition, PredicateCondition::GreaterThanEquals);

  predicate.flip();
  EXPECT_FALSE(predicate.is_flipped());
  EXPECT_EQ(predicate.column_ids.first, ColumnID{0});
  EXPECT_EQ(predicate.column_ids.second, ColumnID{1});
  EXPECT_EQ(predicate.predicate_condition, PredicateCondition::LessThanEquals);
}

}  // namespace hyrise
