#pragma once

#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "types.hpp"

namespace hyrise {

struct TableColumnDefinition final {
  TableColumnDefinition() = default;
  TableColumnDefinition(const std::string& init_name, const DataType init_data_type, const bool init_nullable);

  bool operator==(const TableColumnDefinition& rhs) const;
  size_t hash() const;

  std::string name;
  DataType data_type{DataType::Int};
  bool nullable{false};
};

// So that google test, e.g., prints readable error messages
inline std::ostream& operator<<(std::ostream& stream, const TableColumnDefinition& definition) {
  stream << definition.name << " ";
  stream << definition.data_type << " ";
  stream << (definition.nullable ? "nullable" : "not nullable");
  return stream;
}

using TableColumnDefinitions = std::vector<TableColumnDefinition>;

TableColumnDefinitions concatenated(const TableColumnDefinitions& lhs, const TableColumnDefinitions& rhs);

}  // namespace hyrise
