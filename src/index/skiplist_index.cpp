//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skiplist_index.cpp
//
// Identification: src/index/skiplist_index.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "index/skiplist_index.h"

#include "common/logger.h"
#include "index/index_key.h"
#include "index/scan_optimizer.h"
#include "statistics/stats_aggregator.h"
#include "storage/tuple.h"
#include "index/skiplist.h"

namespace peloton {
namespace index {

SKIPLIST_TEMPLATE_ARGUMENTS
SKIPLIST_INDEX_TYPE::SkipListIndex(IndexMetadata *metadata)
    :  // Base class
      Index{metadata},
      // Key "less than" relation comparator
      comparator{},
      // Key equality checker
      equals{},

      container{comparator, equals} {
  // TODO: I think this is how we decide whether to support duplicate keys
  container.SetSupportDuplicates(!metadata->HasUniqueKeys());

  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
SKIPLIST_INDEX_TYPE::~SkipListIndex() {}

/*
 * InsertEntry() - insert a key-value pair into the map
 *
 * If the key value pair already exists in the map, just return false
 */
SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_INDEX_TYPE::InsertEntry(const storage::Tuple *key,
                                      ItemPointer *value) {
  KeyType index_key;
  index_key.SetFromKey(key);

  bool ret = container.Insert(index_key, value);

  if (ret) {
    LOG_TRACE("Inserted key: %s - [SUCCESS]", key->GetInfo().c_str());
  } else {
    LOG_TRACE("Inserted key: %s - [FAILURE]", key->GetInfo().c_str());
  }

  return ret;
}

/*
 * DeleteEntry() - Removes a key-value pair
 *
 * If the key-value pair does not exists yet in the map return false
 */
SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_INDEX_TYPE::DeleteEntry(const storage::Tuple *key,
                                      ItemPointer *value) {
  KeyType index_key;
  index_key.SetFromKey(key);

  bool ret = container.Remove(index_key, value);

  if (ret) {
    LOG_TRACE("Deleted key: %s - [SUCCESS]", key->GetInfo().c_str());
  } else {
    LOG_TRACE("Deleted key: %s - [FAILURE]", key->GetInfo().c_str());
  }

  return ret;
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_INDEX_TYPE::CondInsertEntry(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE ItemPointer *value,
    UNUSED_ATTRIBUTE std::function<bool(const void *)> predicate) {
  KeyType index_key;
  index_key.SetFromKey(key);

  bool predicate_satisfied = false;

  // This function will complete them in one step
  // predicate will be set to nullptr if the predicate
  // returns true for some value
  bool ret = container.ConditionalInsert(index_key, value, predicate,
                                         &predicate_satisfied);

  // If predicate is not satisfied then we know insertion successes
  if (predicate_satisfied == false) {
    // So it should always succeed?
    assert(ret == true);
    LOG_TRACE("Cond. Inserted Key: %s - [SUCCESS]", key->GetInfo().c_str());
  } else {
    LOG_TRACE("Cond. Inserted Key: %s - [FAILURE]", key->GetInfo().c_str());
    assert(ret == false);
  }

  return ret;
}

/*
 * Scan() - Scans a range inside the index using index scan optimizer
 *
 */
SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_INDEX_TYPE::Scan(
    UNUSED_ATTRIBUTE const std::vector<type::Value> &value_list,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &tuple_column_id_list,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_list,
    UNUSED_ATTRIBUTE ScanDirectionType scan_direction,
    UNUSED_ATTRIBUTE std::vector<ValueType> &result,
    UNUSED_ATTRIBUTE const ConjunctionScanPredicate *csp_p) {
  if (scan_direction == ScanDirectionType::INVALID) {
    throw Exception("Invalid scan direction \n");
  }

  LOG_TRACE("Scan() Point Query = %d; Full Scan = %d ", csp_p->IsPointQuery(),
            csp_p->IsFullIndexScan());

  if (csp_p->IsPointQuery()) {
    // point query
    const auto point_query_key_p = csp_p->GetPointQueryKey();

    KeyType point_query_key;
    point_query_key.SetFromKey(point_query_key_p);

    const auto node = container.FindNode(point_query_key);
    result.push_back(node->kv_p.second);
  } else if (csp_p->IsFullIndexScan()) {
    // full scan
    for (auto scan_itr = container.Begin(); !scan_itr.IsEnd(); scan_itr++) {
      result.push_back(scan_itr->second);
    }
  } else {
    const auto low_key_p = csp_p->GetLowKey();
    const auto high_key_p = csp_p->GetHighKey();

    KeyType index_low_key;
    KeyType index_high_key;
    index_low_key.SetFromKey(low_key_p);
    index_high_key.SetFromKey(high_key_p);

    for (auto scan_itr = container.Begin(index_low_key);
         !scan_itr.IsEnd() &&
             container.key_cmp_less_equal(scan_itr->first, index_high_key);
         scan_itr++) {
      result.push_back(scan_itr->second);
    }
    if (scan_direction == ScanDirectionType::BACKWARD) {
      std::reverse(result.begin(), result.end());
    }
  }
}

/*
 * ScanLimit() - Scan the index with predicate and limit/offset
 *
 */
SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_INDEX_TYPE::ScanLimit(
    UNUSED_ATTRIBUTE const std::vector<type::Value> &value_list,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &tuple_column_id_list,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_list,
    UNUSED_ATTRIBUTE ScanDirectionType scan_direction,
    UNUSED_ATTRIBUTE std::vector<ValueType> &result,
    UNUSED_ATTRIBUTE const ConjunctionScanPredicate *csp_p,
    UNUSED_ATTRIBUTE uint64_t limit, UNUSED_ATTRIBUTE uint64_t offset) {
  if (!csp_p->IsPointQuery() && limit == 1 && offset == 0) {
    const auto low_key_p = csp_p->GetLowKey();
    const auto high_key_p = csp_p->GetHighKey();

    LOG_TRACE("ScanLimit() special case (limit = 1; offset = 0; ASCENDING): %s",
              low_key_p->GetInfo().c_str());

    KeyType index_low_key;
    KeyType index_high_key;
    index_low_key.SetFromKey(low_key_p);
    index_high_key.SetFromKey(high_key_p);

    auto scan_itr = container.Begin(index_low_key);
    if (!scan_itr.IsEnd() &&
        container.key_cmp_less_equal(scan_itr->first, index_high_key)) {
      result.push_back(scan_itr->second);
    } else {
      Scan(value_list, tuple_column_id_list, expr_list, scan_direction, result,
           csp_p);
    }
  }
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_INDEX_TYPE::ScanAllKeys(std::vector<ValueType> &result) {
  auto it = container.Begin();

  // scan all keys
  while (!it.IsEnd()) {
    result.push_back(it->second);
    it++;
  }
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_INDEX_TYPE::ScanKey(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE std::vector<ValueType> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  auto node = container.FindNode(index_key);
  result.push_back(node->kv_p.second);
}

SKIPLIST_TEMPLATE_ARGUMENTS
std::string SKIPLIST_INDEX_TYPE::GetTypeName() const { return "SkipList"; }

// IMPORTANT: Make sure you don't exceed CompactIntegerKey_MAX_SLOTS

template class SkipListIndex<
    CompactIntsKey<1>, ItemPointer *, CompactIntsComparator<1>,
    CompactIntsEqualityChecker<1>, ItemPointerComparator>;
template class SkipListIndex<
    CompactIntsKey<2>, ItemPointer *, CompactIntsComparator<2>,
    CompactIntsEqualityChecker<2>, ItemPointerComparator>;
template class SkipListIndex<
    CompactIntsKey<3>, ItemPointer *, CompactIntsComparator<3>,
    CompactIntsEqualityChecker<3>, ItemPointerComparator>;
template class SkipListIndex<
    CompactIntsKey<4>, ItemPointer *, CompactIntsComparator<4>,
    CompactIntsEqualityChecker<4>, ItemPointerComparator>;

// Generic key
template class SkipListIndex<GenericKey<4>, ItemPointer *,
                             FastGenericComparator<4>,
                             GenericEqualityChecker<4>, ItemPointerComparator>;
template class SkipListIndex<GenericKey<8>, ItemPointer *,
                             FastGenericComparator<8>,
                             GenericEqualityChecker<8>, ItemPointerComparator>;
template class SkipListIndex<GenericKey<16>, ItemPointer *,
                             FastGenericComparator<16>,
                             GenericEqualityChecker<16>, ItemPointerComparator>;
template class SkipListIndex<GenericKey<64>, ItemPointer *,
                             FastGenericComparator<64>,
                             GenericEqualityChecker<64>, ItemPointerComparator>;
template class SkipListIndex<
    GenericKey<256>, ItemPointer *, FastGenericComparator<256>,
    GenericEqualityChecker<256>, ItemPointerComparator>;

// Tuple key
template class SkipListIndex<TupleKey, ItemPointer *, TupleKeyComparator,
                             TupleKeyEqualityChecker, ItemPointerComparator>;

}  // namespace index
}  // namespace peloton
