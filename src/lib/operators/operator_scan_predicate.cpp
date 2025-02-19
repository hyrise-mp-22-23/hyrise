#include "operator_scan_predicate.hpp"

#include "constant_mappings.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace {

using namespace hyrise;                         // NOLINT
using namespace hyrise::expression_functional;  // NOLINT

std::optional<AllParameterVariant> resolve_all_parameter_variant(const AbstractExpression& expression,
                                                                 const AbstractLQPNode& node) {
  if (const auto* value_expression = dynamic_cast<const ValueExpression*>(&expression)) {
    return value_expression->value;
  }
  if (const auto column_id = node.find_column_id(expression)) {
    return *column_id;
  }
  if (const auto* const parameter_expression = dynamic_cast<const CorrelatedParameterExpression*>(&expression)) {
    return parameter_expression->parameter_id;
  }
  if (const auto* const placeholder_expression = dynamic_cast<const PlaceholderExpression*>(&expression)) {
    return placeholder_expression->parameter_id;
  }

  return std::nullopt;
}

}  // namespace

namespace hyrise {

std::ostream& OperatorScanPredicate::output_to_stream(std::ostream& stream,
                                                      const std::shared_ptr<const Table>& table) const {
  std::string column_name_left = std::string("Column #") + std::to_string(column_id);
  if (table) {
    column_name_left = table->column_name(column_id);
  }

  stream << column_name_left << " " << predicate_condition;

  if (table && is_column_id(value)) {
    stream << table->column_name(boost::get<ColumnID>(value));
  } else {
    stream << value;
  }

  if (is_between_predicate_condition(predicate_condition)) {
    stream << " AND " << *value2;
  }

  return stream;
}

std::optional<std::vector<OperatorScanPredicate>> OperatorScanPredicate::from_expression(
    const AbstractExpression& expression, const AbstractLQPNode& node) {
  const auto* predicate = dynamic_cast<const AbstractPredicateExpression*>(&expression);
  if (!predicate) {
    return std::nullopt;
  }

  Assert(!predicate->arguments.empty(), "Expect PredicateExpression to have one or more arguments");

  auto predicate_condition = predicate->predicate_condition;

  auto argument_a = resolve_all_parameter_variant(*predicate->arguments[0], node);
  if (!argument_a) {
    return std::nullopt;
  }

  if (predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull) {
    if (is_column_id(*argument_a)) {
      return std::vector<OperatorScanPredicate>{
          OperatorScanPredicate{boost::get<ColumnID>(*argument_a), predicate_condition}};
    }

    return std::nullopt;
  }

  Assert(predicate->arguments.size() > 1, "Expect non-unary PredicateExpression to have two or more arguments");

  auto argument_b = resolve_all_parameter_variant(*expression.arguments[1], node);
  if (!argument_b) {
    return std::nullopt;
  }

  // We can handle x BETWEEN a AND b if a and b are scalar values of the same data type. Otherwise, the condition gets
  // translated into two scans. Theoretically, we could also implement all variations where x, a and b are
  // non-scalar and of varying types, but as these are used less frequently, would require more code, and increase
  // compile time, we don't do that for now.
  if (is_between_predicate_condition(predicate_condition)) {
    Assert(predicate->arguments.size() == 3, "Expect ternary PredicateExpression to have three arguments");

    auto argument_c = resolve_all_parameter_variant(*expression.arguments[2], node);
    if (!argument_c) {
      return std::nullopt;
    }

    if (is_column_id(*argument_a) && is_variant(*argument_b) && is_variant(*argument_c) &&
        predicate->arguments[1]->data_type() == predicate->arguments[2]->data_type() &&
        !variant_is_null(boost::get<AllTypeVariant>(*argument_b)) &&
        !variant_is_null(boost::get<AllTypeVariant>(*argument_c))) {
      // This is the BETWEEN case that we can handle
      return std::vector<OperatorScanPredicate>{
          OperatorScanPredicate{boost::get<ColumnID>(*argument_a), predicate_condition, *argument_b, *argument_c}};
    }

    PerformanceWarning("BETWEEN handled as two table scans because no BETWEEN specialization was available");

    // We can't handle the case, so we translate it into two predicates
    auto lower_bound_predicates = std::optional<std::vector<OperatorScanPredicate>>{};
    auto upper_bound_predicates = std::optional<std::vector<OperatorScanPredicate>>{};

    if (is_lower_inclusive_between(predicate_condition)) {
      lower_bound_predicates =
          from_expression(*greater_than_equals_(predicate->arguments[0], predicate->arguments[1]), node);
    } else {
      lower_bound_predicates = from_expression(*greater_than_(predicate->arguments[0], predicate->arguments[1]), node);
    }

    if (is_upper_inclusive_between(predicate_condition)) {
      upper_bound_predicates =
          from_expression(*less_than_equals_(predicate->arguments[0], predicate->arguments[2]), node);
    } else {
      upper_bound_predicates = from_expression(*less_than_(predicate->arguments[0], predicate->arguments[2]), node);
    }

    if (!lower_bound_predicates || !upper_bound_predicates) {
      return std::nullopt;
    }

    auto predicates = *lower_bound_predicates;
    predicates.insert(predicates.end(), upper_bound_predicates->begin(), upper_bound_predicates->end());

    return predicates;
  }

  if (!is_column_id(*argument_a)) {
    if (is_column_id(*argument_b)) {
      std::swap(argument_a, argument_b);
      predicate_condition = flip_predicate_condition(predicate_condition);
    } else {
      // Literal-only predicates like "5 > 3" cannot be turned into OperatorScanPredicates
      return std::nullopt;
    }
  }

  return std::vector<OperatorScanPredicate>{
      OperatorScanPredicate{boost::get<ColumnID>(*argument_a), predicate_condition, *argument_b}};
}

OperatorScanPredicate::OperatorScanPredicate(const ColumnID init_column_id,
                                             const PredicateCondition init_predicate_condition,
                                             const AllParameterVariant& init_value,
                                             const std::optional<AllParameterVariant>& init_value2)
    : column_id(init_column_id),
      predicate_condition(init_predicate_condition),
      value(init_value),
      value2(init_value2) {}

bool operator==(const OperatorScanPredicate& lhs, const OperatorScanPredicate& rhs) {
  return lhs.column_id == rhs.column_id && lhs.predicate_condition == rhs.predicate_condition &&
         lhs.value == rhs.value && lhs.value2 == rhs.value2;
}

std::ostream& operator<<(std::ostream& stream, const OperatorScanPredicate& predicate) {
  predicate.output_to_stream(stream);
  return stream;
}

}  // namespace hyrise
