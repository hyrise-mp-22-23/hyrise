#pragma once

#include <memory>
#include <string>
#include <vector>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/abstract_read_only_operator.hpp"

namespace hyrise {

// maintenance operator for the "CREATE VIEW" sql statement
class DropView : public AbstractReadOnlyOperator {
 public:
  DropView(const std::string& init_view_name, bool init_if_exists);

  const std::string& name() const override;
  const std::string view_name;
  const bool if_exists;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
};
}  // namespace hyrise
