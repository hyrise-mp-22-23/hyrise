#include "create_prepared_plan_node.hpp"

#include <sstream>

#include "storage/prepared_plan.hpp"

namespace hyrise {

CreatePreparedPlanNode::CreatePreparedPlanNode(const std::string& init_name,
                                               const std::shared_ptr<PreparedPlan>& init_prepared_plan)
    : AbstractNonQueryNode(LQPNodeType::CreatePreparedPlan), name(init_name), prepared_plan(init_prepared_plan) {}

std::string CreatePreparedPlanNode::description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << "[CreatePreparedPlan] '" << name << "' {\n";
  stream << *prepared_plan;
  stream << "}";

  return stream.str();
}

size_t CreatePreparedPlanNode::_on_shallow_hash() const {
  auto hash = prepared_plan->hash();
  boost::hash_combine(hash, name);
  return hash;
}

std::shared_ptr<AbstractLQPNode> CreatePreparedPlanNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return CreatePreparedPlanNode::make(name, prepared_plan);
}

bool CreatePreparedPlanNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& create_prepared_plan_node = static_cast<const CreatePreparedPlanNode&>(rhs);
  return name == create_prepared_plan_node.name && *prepared_plan == *create_prepared_plan_node.prepared_plan;
}

}  // namespace hyrise
