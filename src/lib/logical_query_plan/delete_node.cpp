#include "delete_node.hpp"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>

#include "utils/assert.hpp"

namespace hyrise {

DeleteNode::DeleteNode() : AbstractNonQueryNode(LQPNodeType::Delete) {}

std::string DeleteNode::description(const DescriptionMode mode) const {
  return "[Delete]";
}

bool DeleteNode::is_column_nullable(const ColumnID column_id) const {
  Fail("Delete does not output any columns");
}

std::vector<std::shared_ptr<AbstractExpression>> DeleteNode::output_expressions() const {
  return {};
}

std::shared_ptr<AbstractLQPNode> DeleteNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return DeleteNode::make();
}

bool DeleteNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  return true;
}

}  // namespace hyrise
