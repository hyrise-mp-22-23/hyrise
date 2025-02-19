#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"
#include "utils/assert.hpp"

namespace hyrise {

class Delete;
class Insert;

/**
 * Operator that updates a subset of columns of a number of rows and from one table with values supplied in another.
 * The first input table must consist of ReferenceSegments and specifies which rows and columns of the referenced table
 * should be updated. This operator uses bag semantics, that is, exactly the referenced cells are updated, and not all
 * rows with similar data.
 * The second input table must have the exact same column layout and number of rows as the first table and contains the
 * data that is used to update the rows specified by the first table.
 *
 * Assumption: The input has been validated before.
 *
 * Note: Update does not support null values at the moment
 */
class Update : public AbstractReadWriteOperator {
 public:
  explicit Update(const std::string& table_to_update_name, const std::shared_ptr<AbstractOperator>& fields_to_update_op,
                  const std::shared_ptr<AbstractOperator>& update_values_op);

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  // Commit happens in Insert and Delete operators
  void _on_commit_records(const CommitID cid) override {}

  // Rollback happens in Insert and Delete operators
  void _on_rollback_records() override {}

 protected:
  const std::string _table_to_update_name;
  std::shared_ptr<Delete> _delete;
  std::shared_ptr<Insert> _insert;
};
}  // namespace hyrise
