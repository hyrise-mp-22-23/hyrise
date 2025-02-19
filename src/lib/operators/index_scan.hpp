#pragma once

#include <memory>

#include "abstract_read_only_operator.hpp"

#include "all_type_variant.hpp"
#include "storage/index/segment_index_type.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "types.hpp"

namespace hyrise {

class Table;
class AbstractTask;

/**
 * Operator that performs a predicate search using indexes
 *
 * Note: Scans only the set of chunks passed to the constructor
 */
class IndexScan : public AbstractReadOnlyOperator {
 public:
  IndexScan(const std::shared_ptr<const AbstractOperator>& input_operator, const SegmentIndexType index_type,
            const std::vector<ColumnID>& left_column_ids, const PredicateCondition predicate_condition,
            const std::vector<AllTypeVariant>& right_values, const std::vector<AllTypeVariant>& right_values2 = {});

  const std::string& name() const final;

  // If set, only the specified chunks will be scanned. See TableScan::excluded_chunk_ids for usage.
  std::vector<ChunkID> included_chunk_ids;

 protected:
  std::shared_ptr<const Table> _on_execute() final;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _validate_input();
  std::shared_ptr<AbstractTask> _create_job(const ChunkID chunk_id, std::mutex& output_mutex);
  RowIDPosList _scan_chunk(const ChunkID chunk_id);

 private:
  const SegmentIndexType _index_type;
  const std::vector<ColumnID> _left_column_ids;
  const PredicateCondition _predicate_condition;
  const std::vector<AllTypeVariant> _right_values;
  const std::vector<AllTypeVariant> _right_values2;

  std::shared_ptr<const Table> _in_table;
  std::shared_ptr<Table> _out_table;
};

}  // namespace hyrise
