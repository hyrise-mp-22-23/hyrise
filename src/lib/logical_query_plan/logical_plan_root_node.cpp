#include "logical_plan_root_node.hpp"

#include <string>

#include "abstract_lqp_node.hpp"

namespace hyrise {

LogicalPlanRootNode::LogicalPlanRootNode() : AbstractLQPNode(LQPNodeType::Root) {}

std::string LogicalPlanRootNode::description(const DescriptionMode mode) const {
  return "[LogicalPlanRootNode]";
}

std::shared_ptr<AbstractLQPNode> LogicalPlanRootNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return make();
}

std::shared_ptr<LQPUniqueConstraints> LogicalPlanRootNode::unique_constraints() const {
  Fail("LogicalPlanRootNode is not expected to be queried for unique constraints.");
}

std::vector<FunctionalDependency> LogicalPlanRootNode::non_trivial_functional_dependencies() const {
  Fail("LogicalPlanRootNode is not expected to be queried for functional dependencies.");
}

bool LogicalPlanRootNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  return true;
}

}  // namespace hyrise
