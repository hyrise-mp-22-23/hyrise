#pragma once

#include "operators/abstract_read_only_operator.hpp"
#include "storage/prepared_plan.hpp"

namespace hyrise {

class CreatePreparedPlan : public AbstractReadOnlyOperator {
 public:
  CreatePreparedPlan(const std::string& prepared_plan_name, const std::shared_ptr<PreparedPlan>& prepared_plan);

  const std::string& name() const override;
  std::string description(DescriptionMode description_mode) const override;

  const std::string& prepared_plan_name() const;
  std::shared_ptr<PreparedPlan> prepared_plan() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

 private:
  const std::string _prepared_plan_name;
  const std::shared_ptr<PreparedPlan> _prepared_plan;
};

}  // namespace hyrise
