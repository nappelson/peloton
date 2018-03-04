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

#include "index/testing_index_util.h"
#include "index/skiplist.h"
#include "index/skiplist_index.h"
#include "index/index_key.h"

namespace peloton {

namespace index {

SkipList<CompactIntsKey<1>, ItemPointer *, CompactIntsComparator<1>,
         CompactIntsEqualityChecker<1>, ItemPointerComparator> *
GetSkipList() {
  return new SkipList<CompactIntsKey<1>, ItemPointer *,
                      CompactIntsComparator<1>, CompactIntsEqualityChecker<1>,
                      ItemPointerComparator>();
};
}

namespace test {

class SkipListGCTests : public PelotonTest {
 protected:
  using SkipListIntKeys = index::SkipList<
      index::CompactIntsKey<1>, ItemPointer *, index::CompactIntsComparator<1>,
      index::CompactIntsEqualityChecker<1>, ItemPointerComparator>;

  void SetUp() override { skip_list_ = index::GetSkipList(); }

  void TearDown() override { delete skip_list_; }

  SkipListIntKeys *skip_list_;

  template <typename T>
  int ContainerSize(T container) {
    auto count = 0;
    for (auto itr = container.begin(); itr != container.end(); itr++) {
      count++;
    }
    return count;
  }

  static void JoinEpochHelper(SkipListIntKeys *skip_list, uint64_t thread_itr) {
    auto &epoch_manager = skip_list->GetEpochManager();
    if (thread_itr > 0) {
      auto current_epoch = epoch_manager.GetCurrentEpochNode();
      current_epoch->active_thread_count.fetch_add(1);
    } else {
      epoch_manager.JoinEpoch();
    }
  }
};

TEST_F(SkipListGCTests, CreateEpochTest) {
  auto &epoch_manager = skip_list_->GetEpochManager();
  auto &epoch_node_list = epoch_manager.GetEpochNodeList();
  auto list_size = ContainerSize(epoch_node_list);
  ASSERT_EQ(list_size, 1);

  epoch_manager.CreateNewEpoch();
  epoch_manager.CreateNewEpoch();
  list_size = ContainerSize(epoch_node_list);
  ASSERT_EQ(list_size, 3);
}

TEST_F(SkipListGCTests, ClearEpochBasicTest) {
  auto &epoch_manager = skip_list_->GetEpochManager();
  auto &epoch_node_list = epoch_manager.GetEpochNodeList();
  epoch_manager.CreateNewEpoch();

  auto list_size = ContainerSize(epoch_node_list);
  ASSERT_EQ(list_size, 2);

  // Clear one epoch
  epoch_manager.ClearEpoch();
  list_size = ContainerSize(epoch_node_list);
  ASSERT_EQ(list_size, 1);

  // Ensure we can't clear epoch if head node is the only epoch
  epoch_manager.ClearEpoch();
  list_size = ContainerSize(epoch_node_list);
  ASSERT_EQ(list_size, 1);
}

TEST_F(SkipListGCTests, ClearEpochActiveThreadTest) {
  auto &epoch_manager = skip_list_->GetEpochManager();
  auto &epoch_node_list = epoch_manager.GetEpochNodeList();

  // Ensure if active thread count >= 1, we can't clear an epoch
  epoch_manager.CreateNewEpoch();
  auto list_size = ContainerSize(epoch_node_list);

  // Size is 2 here
  ASSERT_EQ(list_size, 2);
  auto head_epoch_ptr = epoch_node_list.front();
  head_epoch_ptr->active_thread_count.fetch_add(1);
  epoch_manager.ClearEpoch();

  // This shouldn't work because the current epoch node has one active thread
  list_size = ContainerSize(epoch_node_list);
  ASSERT_EQ(list_size, 2);
  ASSERT_EQ(head_epoch_ptr, epoch_node_list.front());
  ASSERT_EQ(head_epoch_ptr->active_thread_count.load(), 1);
}

TEST_F(SkipListGCTests, JoinEpochTest) {
  auto &epoch_manager = skip_list_->GetEpochManager();
  epoch_manager.CreateNewEpoch();

  // Current thread count for new epoch is 0
  auto epoch_node = epoch_manager.JoinEpoch();
  ASSERT_EQ(epoch_node->active_thread_count.load(), 1);
  ASSERT_EQ(epoch_node, epoch_manager.GetCurrentEpochNode());

  // Make epoch node active thread count negative (signaling that clear may be
  // happening)
  epoch_node->active_thread_count = -1;
  LaunchParallelTest(2, SkipListGCTests::JoinEpochHelper, skip_list_);
  // Even though we are joining, for the sake of this test, the expected value
  // is 0 or 1
  ASSERT_GE(epoch_node->active_thread_count.load(), 0);
}

TEST_F(SkipListGCTests, LeaveEpochTest) {
  auto &epoch_manager = skip_list_->GetEpochManager();
  epoch_manager.CreateNewEpoch();

  // Join to inc active thread count to 1
  auto epoch_node = epoch_manager.JoinEpoch();

  // Join to decrement active thread count to 0
  epoch_manager.LeaveEpoch(epoch_node);
  ASSERT_EQ(epoch_node->active_thread_count.load(), 0);
}

TEST_F(SkipListGCTests, GarbageNodeTest) {
  // Add Garbage Node Tests
  auto &epoch_manager = skip_list_->GetEpochManager();
  const auto node = skip_list_->CreateNode();
  epoch_manager.AddGarbageNode(node);
  auto epoch_node = epoch_manager.GetCurrentEpochNode();
  auto garbage_node = epoch_node->garbage_list_ptr.load();
  ASSERT_EQ(garbage_node->node_ptr, node);
  ASSERT_EQ(garbage_node->next_ptr, nullptr);

  // Test that garbage nodes can chain together
  const auto node_2 = skip_list_->CreateNode();
  epoch_manager.AddGarbageNode(node_2);
  garbage_node = epoch_node->garbage_list_ptr.load();
  ASSERT_EQ(garbage_node->node_ptr, node_2);
  ASSERT_EQ(garbage_node->next_ptr->node_ptr, node);

  // Test to see if clearing the epoch does the right thing
  epoch_manager.CreateNewEpoch();
  epoch_manager.ClearEpoch();
  ASSERT_EQ(ContainerSize(epoch_manager.GetEpochNodeList()), 1);
}

}  // namespace test
}  // namespace peloton
