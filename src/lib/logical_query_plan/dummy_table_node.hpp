#pragma once

#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"

namespace hyrise {

/**
 * This node type represents a dummy table that is used to project literals.
 * See Projection::DummyTable for more details.
 */
class DummyTableNode : public EnableMakeForLQPNode<DummyTableNode>, public AbstractLQPNode {
 public:
  DummyTableNode();

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  std::vector<std::shared_ptr<AbstractExpression>> output_expressions() const override;
  bool is_column_nullable(const ColumnID column_id) const override;
  std::shared_ptr<LQPUniqueConstraints> unique_constraints() const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace hyrise
