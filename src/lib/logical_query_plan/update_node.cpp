#include "update_node.hpp"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace hyrise {

UpdateNode::UpdateNode(const std::string& init_table_name)
    : AbstractNonQueryNode(LQPNodeType::Update), table_name(init_table_name) {}

std::string UpdateNode::description(const DescriptionMode mode) const {
  std::ostringstream desc;

  desc << "[Update] Table: '" << table_name << "'";

  return desc.str();
}

std::shared_ptr<AbstractLQPNode> UpdateNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return UpdateNode::make(table_name);
}

bool UpdateNode::is_column_nullable(const ColumnID column_id) const {
  Fail("Update does not output any colums");
}

std::vector<std::shared_ptr<AbstractExpression>> UpdateNode::output_expressions() const {
  return {};
}

size_t UpdateNode::_on_shallow_hash() const {
  return boost::hash_value(table_name);
}

bool UpdateNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& update_node_rhs = static_cast<const UpdateNode&>(rhs);
  return table_name == update_node_rhs.table_name;
}

}  // namespace hyrise
