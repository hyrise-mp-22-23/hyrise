#pragma once

#include <memory>

#include <boost/hana/contains.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/type.hpp>

#include "fixed_width_integer_decompressor.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "types.hpp"

namespace hyrise {

namespace hana = boost::hana;

/**
 * @brief Stores values as either uint32_t, uint16_t, or uint8_t
 *
 * This is simplest vector compression scheme. It matches the old FittedAttributeVector
 */
template <typename UnsignedIntType>
class FixedWidthIntegerVector : public CompressedVector<FixedWidthIntegerVector<UnsignedIntType>> {
  static_assert(hana::contains(hana::tuple_t<uint8_t, uint16_t, uint32_t>, hana::type_c<UnsignedIntType>),
                "UnsignedIntType must be any of the three listed unsigned integer types.");

 public:
  explicit FixedWidthIntegerVector(pmr_vector<UnsignedIntType> data)
      : _data{std::move(data)}, _data_span{_data.data(), _data.size()} {}

  explicit FixedWidthIntegerVector(const std::span<const UnsignedIntType> data_span) : _data_span{data_span} {}

  const pmr_vector<UnsignedIntType>& data() const {
    return _data;
  }

 public:
  size_t on_size() const {
    return _data_span.size();
  }

  size_t on_data_size() const {
    return sizeof(UnsignedIntType) * _data_span.size();
  }

  auto on_create_base_decompressor() const {
    return std::make_unique<FixedWidthIntegerDecompressor<UnsignedIntType>>(_data_span);
  }

  auto on_create_decompressor() const {
    return FixedWidthIntegerDecompressor<UnsignedIntType>(_data_span);
  }

  auto on_begin() const {
    return _data_span.begin();
  }

  auto on_end() const {
    return _data_span.end();
  }

  std::unique_ptr<const BaseCompressedVector> on_copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
    auto data_copy = pmr_vector<UnsignedIntType>{_data, alloc};
    return std::make_unique<FixedWidthIntegerVector<UnsignedIntType>>(std::move(data_copy));
  }

 private:
  const pmr_vector<UnsignedIntType> _data;
  const std::span<const UnsignedIntType> _data_span;
};

}  // namespace hyrise
