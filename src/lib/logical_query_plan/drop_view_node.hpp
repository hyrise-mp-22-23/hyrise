#pragma once

#include <memory>
#include <string>

#include "abstract_non_query_node.hpp"

namespace hyrise {

/**
 * Node type to represent deleting a view from the StorageManager
 */
class DropViewNode : public EnableMakeForLQPNode<DropViewNode>, public AbstractNonQueryNode {
 public:
  DropViewNode(const std::string& init_view_name, bool init_if_exists);

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  const std::string view_name;
  const bool if_exists;

 protected:
  size_t _on_shallow_hash() const override;
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace hyrise
