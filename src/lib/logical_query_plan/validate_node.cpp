#include "validate_node.hpp"

#include <string>

namespace hyrise {

ValidateNode::ValidateNode() : AbstractLQPNode(LQPNodeType::Validate) {}

std::string ValidateNode::description(const DescriptionMode mode) const {
  return "[Validate]";
}

std::shared_ptr<LQPUniqueConstraints> ValidateNode::unique_constraints() const {
  return _forward_left_unique_constraints();
}

std::shared_ptr<AbstractLQPNode> ValidateNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return ValidateNode::make();
}

bool ValidateNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  return true;
}

}  // namespace hyrise
