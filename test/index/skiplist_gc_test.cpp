//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skiplist_gc_test.cpp
//
// Identification: test/index/skiplist_gc_test.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "gtest/gtest.h"

#include "common/internal_types.h"
#include "index/testing_index_util.h"
#include "index/skiplist.h"
#include "index/skiplist_index.h"
#include "index/index_factory.h"
#include "index/index_key.h"

namespace peloton {

namespace index {

SkipListIndex<CompactIntsKey<1>, ItemPointer *, CompactIntsComparator<1>,
              CompactIntsEqualityChecker<1>, ItemPointerComparator> *
GetSkipListIndex(IndexMetadata *metadata) {
  return new SkipListIndex<
      CompactIntsKey<1>, ItemPointer *, CompactIntsComparator<1>,
      CompactIntsEqualityChecker<1>, ItemPointerComparator>(metadata);
}

SkipList<CompactIntsKey<1>, ItemPointer *, CompactIntsComparator<1>,
         CompactIntsEqualityChecker<1>, ItemPointerComparator> *
GetSkipList() {
  return new SkipList<CompactIntsKey<1>, ItemPointer *,
                      CompactIntsComparator<1>, CompactIntsEqualityChecker<1>,
                      ItemPointerComparator>();
};
}

namespace test {

class SkipListGCTests : public PelotonTest {};

TEST_F(SkipListGCTests, BasicTest) {
  auto skip_list = index::GetSkipList();
  auto &epoch_manager = skip_list->GetEpochManager();
  epoch_manager.CreateNewEpoch();
}

}  // namespace test
}  // namespace peloton
