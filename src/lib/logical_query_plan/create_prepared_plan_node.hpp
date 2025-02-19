#pragma once

#include "logical_query_plan/abstract_non_query_node.hpp"

namespace hyrise {

class PreparedPlan;

/**
 * LQP equivalent to the PrepareStatement operator.
 */
class CreatePreparedPlanNode : public EnableMakeForLQPNode<CreatePreparedPlanNode>, public AbstractNonQueryNode {
 public:
  CreatePreparedPlanNode(const std::string& init_name, const std::shared_ptr<PreparedPlan>& init_prepared_plan);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  std::string name;
  std::shared_ptr<PreparedPlan> prepared_plan;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace hyrise
