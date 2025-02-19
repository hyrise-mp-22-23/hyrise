#include "value_expression.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>
#include "resolve_type.hpp"

namespace hyrise {

ValueExpression::ValueExpression(const AllTypeVariant& init_value)
    : AbstractExpression(ExpressionType::Value, {}), value(init_value) {}

bool ValueExpression::requires_computation() const {
  return false;
}

std::shared_ptr<AbstractExpression> ValueExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<ValueExpression>(value);
}

std::string ValueExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;

  if (value.type() == typeid(pmr_string)) {
    stream << "'" << value << "'";
  } else {
    stream << value;
  }

  if (value.type() == typeid(int64_t)) {
    stream << "L";
  } else if (value.type() == typeid(float)) {
    stream << "F";
  }

  return stream.str();
}

DataType ValueExpression::data_type() const {
  return data_type_from_all_type_variant(value);
}

bool ValueExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const ValueExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& value_expression = static_cast<const ValueExpression&>(expression);

  /**
   * Even though null != null, two null expressions are *the same expressions* (e.g. when resolving ColumnIDs)
   */
  if (data_type() == DataType::Null && value_expression.data_type() == DataType::Null) {
    return true;
  }

  return value == value_expression.value;
}

size_t ValueExpression::_shallow_hash() const {
  return std::hash<AllTypeVariant>{}(value);
}

bool ValueExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  return value.type() == typeid(NullValue);
}

}  // namespace hyrise
