#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"

namespace hyrise {

/**
 * Assign column names to expressions
 */
class AliasNode : public EnableMakeForLQPNode<AliasNode>, public AbstractLQPNode {
 public:
  AliasNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
            const std::vector<std::string>& init_aliases);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;
  std::vector<std::shared_ptr<AbstractExpression>> output_expressions() const override;

  // Forwards unique constraints from the left input node
  std::shared_ptr<LQPUniqueConstraints> unique_constraints() const override;

  const std::vector<std::string> aliases;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace hyrise
