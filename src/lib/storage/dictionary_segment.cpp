#include "dictionary_segment.hpp"

#include <memory>
#include <string>

#include "resolve_type.hpp"
#include "storage/storage_manager.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "utils/size_estimation_utils.hpp"

namespace {

using namespace hyrise;  // NOLINT

template <typename T>
void export_value(const T& value, std::ofstream& ofstream) {
  ofstream.write(reinterpret_cast<const char*>(&value), sizeof(T));
}

template <typename T>
void export_values(const std::span<const T>& data_span, std::ofstream& ofstream) {
  ofstream.write(reinterpret_cast<const char*>(data_span.data()), data_span.size() * sizeof(T));
}

template <typename T, typename Alloc>
void export_values(const std::vector<T, Alloc>& values, std::ofstream& ofstream) {
  ofstream.write(reinterpret_cast<const char*>(values.data()), values.size() * sizeof(T));
}

// needed for attribute vector which is stored in a compact manner
void export_compact_vector(const pmr_compact_vector& values, std::ofstream& ofstream) {
  export_value(values.bits(), ofstream);
  ofstream.write(reinterpret_cast<const char*>(values.get()), static_cast<int64_t>(values.bytes()));
}

void export_compressed_vector(const CompressedVectorType type, const BaseCompressedVector& compressed_vector,
                              std::ofstream& ofstream) {
  switch (type) {
    case CompressedVectorType::FixedWidthInteger4Byte:
      export_values(dynamic_cast<const FixedWidthIntegerVector<uint32_t>&>(compressed_vector).data(), ofstream);
      return;
    case CompressedVectorType::FixedWidthInteger2Byte:
      export_values(dynamic_cast<const FixedWidthIntegerVector<uint16_t>&>(compressed_vector).data(), ofstream);
      return;
    case CompressedVectorType::FixedWidthInteger1Byte:
      export_values(dynamic_cast<const FixedWidthIntegerVector<uint8_t>&>(compressed_vector).data(), ofstream);
      return;
    case CompressedVectorType::BitPacking:
      export_compact_vector(dynamic_cast<const BitPackingVector&>(compressed_vector).data(), ofstream);
      return;
    default:
      Fail("Any other type should have been caught before.");
  }
}

}  // namespace

namespace hyrise {

template <typename T>
DictionarySegment<T>::DictionarySegment(const std::shared_ptr<const pmr_vector<T>>& dictionary,
                                        const std::shared_ptr<const BaseCompressedVector>& attribute_vector)
    : BaseDictionarySegment(data_type_from_type<T>()),
      _dictionary_base_vector{dictionary},
      _dictionary{
          std::make_shared<std::span<const T>>(_dictionary_base_vector->data(), _dictionary_base_vector->size())},
      _attribute_vector{attribute_vector},
      _decompressor{_attribute_vector->create_base_decompressor()} {
  // NULL is represented by _dictionary_base_vector.size(). INVALID_VALUE_ID, which is the highest possible number in
  // ValueID::base_type (2^32 - 1), is needed to represent "value not found" in calls to lower_bound/upper_bound.
  // For a DictionarySegment of the max size Chunk::MAX_SIZE, those two values overlap.
  Assert(_dictionary_base_vector->size() < std::numeric_limits<ValueID::base_type>::max(), "Input segment too big");
}

template <typename T>
DictionarySegment<T>::DictionarySegment(const std::shared_ptr<const std::span<const T>>& dictionary,
                                        const std::shared_ptr<const BaseCompressedVector>& attribute_vector)
    : BaseDictionarySegment(data_type_from_type<T>()),
      _dictionary_base_vector{},
      _dictionary{dictionary},
      _attribute_vector{attribute_vector},
      _decompressor{_attribute_vector->create_base_decompressor()} {
  // NULL is represented by _dictionary_base_vector.size(). INVALID_VALUE_ID, which is the highest possible number in
  // ValueID::base_type (2^32 - 1), is needed to represent "value not found" in calls to lower_bound/upper_bound.
  // For a DictionarySegment of the max size Chunk::MAX_SIZE, those two values overlap.
  Assert(_dictionary->size() < std::numeric_limits<ValueID::base_type>::max(), "Input segment too big");
}

template <typename T>
DictionarySegment<T>::DictionarySegment(const std::byte* start_address)
    : BaseDictionarySegment(data_type_from_type<T>()) {
  const auto header_data = reinterpret_cast<const uint32_t*>(start_address);
  const auto encoding_type = PersistedSegmentEncodingType{header_data[ENCODING_TYPE_OFFSET_INDEX]};
  const auto dictionary_size = header_data[DICTIONARY_SIZE_OFFSET_INDEX];
  const auto attribute_vector_size = header_data[ATTRIBUTE_VECTOR_OFFSET_INDEX];

  auto* dictionary_address = reinterpret_cast<const T*>(start_address + HEADER_OFFSET_BYTES);
  auto dictionary_span_pointer = std::make_shared<std::span<const T>>(dictionary_address, dictionary_size);
  const auto dictionary_size_bytes = dictionary_size * sizeof(T);

  switch (encoding_type) {
    case PersistedSegmentEncodingType::Unencoded: {
      Fail("UnencodedSegments aren't supported for initialization from mmap-based storage yet.");
    }
    case PersistedSegmentEncodingType::DictionaryEncoding8Bit: {
      auto* const attribute_vector_address =
          reinterpret_cast<const uint8_t*>(start_address + HEADER_OFFSET_BYTES + dictionary_size_bytes);
      auto attribute_data_span = std::span<const uint8_t>(attribute_vector_address, attribute_vector_size);
      auto attribute_vector = std::make_shared<FixedWidthIntegerVector<uint8_t>>(attribute_data_span);

      _dictionary = dictionary_span_pointer;
      _attribute_vector = attribute_vector;
      _decompressor = _attribute_vector->create_base_decompressor();

      break;
    }
    case PersistedSegmentEncodingType::DictionaryEncoding16Bit: {
      auto* const attribute_vector_address =
          reinterpret_cast<const uint16_t*>(start_address + HEADER_OFFSET_BYTES + dictionary_size_bytes);
      auto attribute_data_span = std::span<const uint16_t>(attribute_vector_address, attribute_vector_size);
      auto attribute_vector = std::make_shared<FixedWidthIntegerVector<uint16_t>>(attribute_data_span);

      _dictionary = dictionary_span_pointer;
      _attribute_vector = attribute_vector;
      _decompressor = _attribute_vector->create_base_decompressor();

      break;
    }
    case PersistedSegmentEncodingType::DictionaryEncoding32Bit: {
      auto* const attribute_vector_address =
          reinterpret_cast<const uint32_t*>(start_address + HEADER_OFFSET_BYTES + dictionary_size_bytes);
      auto attribute_data_span = std::span<const uint32_t>(attribute_vector_address, attribute_vector_size);
      auto attribute_vector = std::make_shared<FixedWidthIntegerVector<uint32_t>>(attribute_data_span);

      _dictionary = dictionary_span_pointer;
      _attribute_vector = attribute_vector;
      _decompressor = _attribute_vector->create_base_decompressor();

      break;
    }
    case PersistedSegmentEncodingType::DictionaryEncodingBitPacking: {
      Fail("Support for span-based BitPackingVectors for DictionarySegments not implemented yet.");
      break;
    }
    default: {
      Fail("Unsupported EncodingType.");
      break;
    }
  }
}

template <typename T>
AllTypeVariant DictionarySegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] += 1;
  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value) {
    return NULL_VALUE;
  }
  return *typed_value;
}

template <typename T>
std::shared_ptr<const std::span<const T>> DictionarySegment<T>::dictionary() const {
  // We have no idea how the dictionary will be used, so we do not increment the access counters here
  return _dictionary;
}

template <typename T>
ChunkOffset DictionarySegment<T>::size() const {
  return static_cast<ChunkOffset>(_attribute_vector->size());
}

template <typename T>
std::shared_ptr<AbstractSegment> DictionarySegment<T>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  Assert(_dictionary_base_vector, "Cannot copy based on span-only DictionarySegment.");
  auto new_attribute_vector = _attribute_vector->copy_using_allocator(alloc);
  auto new_dictionary = std::make_shared<pmr_vector<T>>(*_dictionary_base_vector, alloc);
  auto copy = std::make_shared<DictionarySegment<T>>(std::move(new_dictionary), std::move(new_attribute_vector));
  copy->access_counter = access_counter;
  return copy;
}

template <typename T>
size_t DictionarySegment<T>::memory_usage(const MemoryUsageCalculationMode mode) const {
  const auto common_elements_size = sizeof(*this) + _attribute_vector->data_size();

  if constexpr (std::is_same_v<T, pmr_string>) {
    //this cannot be a mmap-based DictionarySegment -> we only allow mapping FixedStringDictionarySegments
    //therefore dictionary_base_vector will always exist
    return common_elements_size + string_vector_memory_usage(*_dictionary_base_vector, mode);
  }
  return common_elements_size + _dictionary->size() * sizeof(typename decltype(_dictionary)::element_type::value_type);
}

template <typename T>
std::optional<CompressedVectorType> DictionarySegment<T>::compressed_vector_type() const {
  return _attribute_vector->type();
}

template <typename T>
EncodingType DictionarySegment<T>::encoding_type() const {
  return EncodingType::Dictionary;
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] +=
      static_cast<uint64_t>(std::ceil(std::log2(_dictionary->size())));
  const auto typed_value = boost::get<T>(value);

  auto iter = std::lower_bound(_dictionary->begin(), _dictionary->end(), typed_value);
  if (iter == _dictionary->end()) {
    return INVALID_VALUE_ID;
  }
  return ValueID{static_cast<ValueID::base_type>(std::distance(_dictionary->begin(), iter))};
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] +=
      static_cast<uint64_t>(std::ceil(std::log2(_dictionary->size())));
  const auto typed_value = boost::get<T>(value);

  auto iter = std::upper_bound(_dictionary->begin(), _dictionary->end(), typed_value);
  if (iter == _dictionary->end()) {
    return INVALID_VALUE_ID;
  }
  return ValueID{static_cast<ValueID::base_type>(std::distance(_dictionary->begin(), iter))};
}

template <typename T>
AllTypeVariant DictionarySegment<T>::value_of_value_id(const ValueID value_id) const {
  DebugAssert(value_id < _dictionary->size(), "ValueID out of bounds");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] += 1;
  return (*_dictionary)[value_id];
}

template <typename T>
ValueID::base_type DictionarySegment<T>::unique_values_count() const {
  return static_cast<ValueID::base_type>(_dictionary->size());
}

template <typename T>
std::shared_ptr<const BaseCompressedVector> DictionarySegment<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
ValueID DictionarySegment<T>::null_value_id() const {
  return ValueID{static_cast<ValueID::base_type>(_dictionary->size())};
}

template <typename T>
void DictionarySegment<T>::serialize(std::ofstream& ofstream) const {
  const auto compressed_vector_type_id =
      StorageManager::resolve_persisted_segment_encoding_type_from_compression_type(compressed_vector_type().value());
  export_value(static_cast<uint32_t>(compressed_vector_type_id), ofstream);
  export_value(static_cast<uint32_t>(dictionary()->size()), ofstream);
  export_value(static_cast<uint32_t>(attribute_vector()->size()), ofstream);

  // // we need to ensure that every part can be mapped with a uint32_t map
  export_values<T>(*dictionary(), ofstream);
  export_compressed_vector(*compressed_vector_type(), *attribute_vector(), ofstream);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DictionarySegment);

}  // namespace hyrise
