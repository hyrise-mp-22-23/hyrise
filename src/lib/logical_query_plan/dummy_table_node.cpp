#include "dummy_table_node.hpp"

#include <optional>
#include <string>
#include <vector>

#include "expression/value_expression.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

DummyTableNode::DummyTableNode() : AbstractLQPNode(LQPNodeType::DummyTable) {}

std::string DummyTableNode::description(const DescriptionMode mode) const {
  return "[DummyTable]";
}

std::vector<std::shared_ptr<AbstractExpression>> DummyTableNode::output_expressions() const {
  return {};
}

bool DummyTableNode::is_column_nullable(const ColumnID column_id) const {
  Fail("DummyTable does not output any columns");
}

std::shared_ptr<LQPUniqueConstraints> DummyTableNode::unique_constraints() const {
  return std::make_shared<LQPUniqueConstraints>();
}

std::shared_ptr<AbstractLQPNode> DummyTableNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return std::make_shared<DummyTableNode>();
}

bool DummyTableNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  return true;
}

}  // namespace hyrise
