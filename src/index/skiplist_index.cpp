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
    // value equality checker
    value_equals{},
    // TODO: Support allowDupKey
    container{comparator, equals, value_equals, !(metadata->HasUniqueKeys())} {

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
bool SKIPLIST_INDEX_TYPE::InsertEntry(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE ItemPointer *value) {
  KeyType index_key;
  index_key.SetFromKey(key);

  bool ret = container.insert(index_key, value);

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexInserts(metadata);
  }

  return ret;
}

/*
 * DeleteEntry() - Removes a key-value pair
 *
 * If the key-value pair does not exists yet in the map return false
 */
SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_INDEX_TYPE::DeleteEntry(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE ItemPointer *value) {
  KeyType index_key;
  index_key.SetFromKey(key);
  size_t delete_count = 0;

  // In Delete() since we just use the value for comparison (i.e. read-only)
  // it is unnecessary for us to allocate memory
  bool ret = container.delete_key(index_key, value);

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexDeletes(
        delete_count, metadata);
  }
  return ret;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_INDEX_TYPE::ScanKey(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE std::vector<ValueType> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  // This function in BwTree fills a given vector
  container.GetValue(index_key, result);

//  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
//    stats::BackendStatsContext::GetInstance()->IncrementIndexReads(
//        result.size(), metadata);
//  }

//  LOG_DEBUG("skiplist_index ScanKey() returning with size %lu\n", result.size());

  return;
}



SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_INDEX_TYPE::CondInsertEntry(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE ItemPointer *value,
    UNUSED_ATTRIBUTE std::function<bool(const void *)> predicate) {
  bool ret = false;
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
  // This is a hack - we do not support backward scan
  if (scan_direction == ScanDirectionType::INVALID) {
    throw Exception("Invalid scan direction \n");
  }

  LOG_TRACE("Scan() Point Query = %d; Full Scan = %d ", csp_p->IsPointQuery(),
            csp_p->IsFullIndexScan());

  if (csp_p->IsPointQuery() == true) {

//    LOG_DEBUG("point query detected");
    const storage::Tuple *point_query_key_p = csp_p->GetPointQueryKey();

    KeyType point_query_key;
    point_query_key.SetFromKey(point_query_key_p);

    // Note: We could call ScanKey() to achieve better modularity
    // (slightly less code), but since ScanKey() is a virtual function
    // this would induce an overhead for point query, which must be highly
    // optimized and super fast
    container.GetValue(point_query_key, result);
  } else if (csp_p->IsFullIndexScan() == true) {
    // If it is a full index scan, then just do the scan
    // until we have reached the end of the index by the same
    // we take the snapshot of the last leaf node
    for (auto scan_itr = container.begin(); (scan_itr != nullptr);
         scan_itr = scan_itr->get_right_node()) {
      result.push_back(scan_itr->item_value);
    }  // for it from begin() to end()
  } else {
    const storage::Tuple *low_key_p = csp_p->GetLowKey();
    const storage::Tuple *high_key_p = csp_p->GetHighKey();

    LOG_TRACE("Partial scan low key: %s\n high key: %s",
              low_key_p->GetInfo().c_str(), high_key_p->GetInfo().c_str());

    // Construct low key and high key in KeyType form, rather than
    // the standard in-memory tuple
    KeyType index_low_key;
    KeyType index_high_key;
    index_low_key.SetFromKey(low_key_p);
    index_high_key.SetFromKey(high_key_p);

    // We use bwtree Begin() to first reach the lower bound
    // of the search key
    // Also we keep scanning until we have reached the end of the index
    // or we have seen a key higher than the high key
    for (auto cursor = container.begin(index_low_key);
         (cursor != nullptr) && (!cursor->is_footer_node()) && (container.KeyCmpLessEqual(cursor->node_key, index_high_key));
         cursor = cursor->get_right_node()) {
      result.push_back(cursor->item_value);
    }
  }  // if is full scan

  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexReads(
        result.size(), metadata);
  }

  return;

  return;
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
  // Only work with limit == 1 and offset == 0
  // Because that gets translated to "min"
  // But still since we could not access tuples in the table
  // the index just fetches the first qualified key without further checking
  // including checking for non-exact bounds!!!

  // TODO: support BACKWARD
  // TODO: support offset
  if (csp_p->IsPointQuery() == false && limit == 1 && offset == 0 &&
      scan_direction == ScanDirectionType::FORWARD) {
    const storage::Tuple *low_key_p = csp_p->GetLowKey();
    const storage::Tuple *high_key_p = csp_p->GetHighKey();

    LOG_TRACE("ScanLimit() special case (limit = 1; offset = 0; ASCENDING): %s",
              low_key_p->GetInfo().c_str());

    KeyType index_low_key;
    KeyType index_high_key;
    index_low_key.SetFromKey(low_key_p);
    index_high_key.SetFromKey(high_key_p);

    auto scan_itr = container.begin(index_low_key);
    if ((scan_itr != nullptr) &&
        (container.KeyCmpLessEqual(scan_itr->node_key, index_high_key))) {

      result.push_back(scan_itr->item_value);
    }
  } else {
    Scan(value_list, tuple_column_id_list, expr_list, scan_direction, result,
         csp_p);
  }

  return;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_INDEX_TYPE::ScanAllKeys(
    UNUSED_ATTRIBUTE std::vector<ValueType> &result) {
  auto cursor = container.begin();
  while(cursor != nullptr) {
    result.push_back(cursor->item_value);
    cursor = container.next(cursor);
  }
  if (FLAGS_stats_mode != STATS_TYPE_INVALID) {
    stats::BackendStatsContext::GetInstance()->IncrementIndexReads(
        result.size(), metadata);
  }

//  LOG_DEBUG("skiplist_index ScanAllKeys() returning with size %lu\n", result.size());

  return;
}


SKIPLIST_TEMPLATE_ARGUMENTS
std::string SKIPLIST_INDEX_TYPE::GetTypeName() const { return "SkipList"; }

// IMPORTANT: Make sure you don't exceed CompactIntegerKey_MAX_SLOTS

template
class SkipListIndex<
    CompactIntsKey<1>, ItemPointer *, CompactIntsComparator<1>,
    CompactIntsEqualityChecker<1>, ItemPointerComparator>;
template
class SkipListIndex<
    CompactIntsKey<2>, ItemPointer *, CompactIntsComparator<2>,
    CompactIntsEqualityChecker<2>, ItemPointerComparator>;
template
class SkipListIndex<
    CompactIntsKey<3>, ItemPointer *, CompactIntsComparator<3>,
    CompactIntsEqualityChecker<3>, ItemPointerComparator>;
template
class SkipListIndex<
    CompactIntsKey<4>, ItemPointer *, CompactIntsComparator<4>,
    CompactIntsEqualityChecker<4>, ItemPointerComparator>;

// Generic key
template
class SkipListIndex<GenericKey<4>, ItemPointer *,
                    FastGenericComparator<4>,
                    GenericEqualityChecker<4>, ItemPointerComparator>;
template
class SkipListIndex<GenericKey<8>, ItemPointer *,
                    FastGenericComparator<8>,
                    GenericEqualityChecker<8>, ItemPointerComparator>;
template
class SkipListIndex<GenericKey<16>, ItemPointer *,
                    FastGenericComparator<16>,
                    GenericEqualityChecker<16>, ItemPointerComparator>;
template
class SkipListIndex<GenericKey<64>, ItemPointer *,
                    FastGenericComparator<64>,
                    GenericEqualityChecker<64>, ItemPointerComparator>;
template
class SkipListIndex<
    GenericKey<256>, ItemPointer *, FastGenericComparator<256>,
    GenericEqualityChecker<256>, ItemPointerComparator>;

// Tuple key
template
class SkipListIndex<TupleKey, ItemPointer *, TupleKeyComparator,
                    TupleKeyEqualityChecker, ItemPointerComparator>;

}  // End index namespace
}  // End peloton namespace
