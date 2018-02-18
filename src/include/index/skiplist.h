//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skiplist.h
//
// Identification: src/include/index/skiplist.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <memory>
#include <stdlib.h>
#include <strings.h>
#include <thread>
#include <vector>

#include "common/logger.h"
#include "common/macros.h"

namespace peloton {
namespace index {

// This is the value we use in epoch manager to make sure
// no thread sneaking in while GC decision is being made
#define MAX_THREAD_COUNT ((int)0x7FFFFFFF)

/*
 * SKIPLIST_TEMPLATE_ARGUMENTS - Save some key strokes
 */
#define SKIPLIST_TEMPLATE_ARGUMENTS                                       \
  template <typename KeyType, typename ValueType, typename KeyComparator, \
            typename KeyEqualityChecker, typename ValueEqualityChecker>

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker, typename ValueEqualityChecker>
class SkipList {
 public:
  explicit inline SkipList(
      KeyComparator p_key_cmp_obj = KeyComparator{},
      KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{},
      ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{})
      : key_cmp_obj_{p_key_cmp_obj},
        key_eq_obj_{p_key_eq_obj},
        value_eq_obj_{p_value_eq_obj} {}

  /*
   * Insert
   */
  bool insert(const KeyType &key, const ValueType &value);

  /*
   * Delete
   */
  bool remove(const KeyType &key);

  /*
   * Search
   */
  const ValueType *search(const KeyType &key) const;

  /*
   * Exists
   */
  bool exists(const KeyType &key) const;

 private:
  struct node {
    // The key
    KeyType key_;

    // The value
    ValueType value_;

    // For each level, store the next node
    std::vector<node *> next_node;

    // Deleted bit for this node (logically removed)
    // TODO (steal last bit from next pointer to see deleted bit)
    bool deleted;
  };

  KeyComparator key_cmp_obj_;
  KeyEqualityChecker key_eq_obj_;
  ValueEqualityChecker value_eq_obj_;

  unsigned int max_level_;
  node *root_;

  inline unsigned int generate_level() const {
    return ffs(rand() & ((1 << max_level_) - 1));
  }

 public:
  class ForwardIterator;

  /*
   * Return iterator to beginning of skip list
   */
  ForwardIterator begin() { return ForwardIterator{this}; }

  /*
   * Return iterator using a given key
   */
  ForwardIterator begin(const KeyType &start_key) {
    return ForwardIterator{this, start_key};
  }

 public:
  /*
   * Iterator used for traversing all nodes of skiplist (used at base level)
   */
  class ForwardIterator {
   private:
    node *curr_node_;

   public:
    /*
     * Default Constructor
     */
    ForwardIterator(){};

    /*
     * Constructor given a SkipList
     */
    ForwardIterator(SkipList *skip_list);

    /*
     * Constructor - Construct an iterator given a key
     */
    ForwardIterator(SkipList *skip_list, const KeyType &start_key);

    /*
     * Copy Constructor
     */
    ForwardIterator(const ForwardIterator &other);

    /*
     * Move Assignment
     */
    ForwardIterator &operator=(ForwardIterator &&other);

    /*
    * Destructor
    */
    ~ForwardIterator();

    ///////////////////////////////////////////////////////////////////
    // Operators
    ///////////////////////////////////////////////////////////////////

    /*
    * operator= Assigns one object to another
    */
    ForwardIterator &operator=(const ForwardIterator &other);

    /*
     * operator==() Compares whether two iterators refer to the same key
     */
    inline bool operator==(const ForwardIterator &other) const;

    /*
     * operator< Compares two iterators by comparing their current key
     */
    inline bool operator<(const ForwardIterator &other) const;

    /*
     * Prefix operator++
     */
    inline ForwardIterator &operator++();

    /*
     * Prefix operator--
     */
    inline ForwardIterator &operator--();

    /*
     * Postfix operator++
     */
    inline ForwardIterator operator++(int);

    /*
     * Postfix operator--
     */
    inline ForwardIterator operator--(int);

    /*
    * operator * - Return the value reference currently pointed to by this
    * iterator
    */
    inline const node &operator*() { return *curr_node_; }

    /*
     * operator -> - Return the value pointer pointed to by this iterator
     */
    inline const node *operator->() { return &*curr_node_; }

   private:
    /*
     * Step forward one in skip list
     */
    void step_forward();
  };

  ///////////////////////////////////////////////////////////////////
  // Key Comparison Member Functions
  ///////////////////////////////////////////////////////////////////
 private:
  inline bool key_cmp_less(const KeyType &key1, const KeyType &key2) const {
    return key_cmp_obj_(key1, key2);
  }

  inline bool key_cmp_equal(const KeyType &key1, const KeyType &key2) const {
    return key_eq_obj_(key1, key2);
  }

  inline bool key_cmp_greater_equal(const KeyType &key1,
                                    const KeyType &key2) const {
    return !key_cmp_less(key1, key2);
  }

  inline bool key_cmp_greater(const KeyType &key1, const KeyType &key2) const {
    return key_cmp_less(key2, key1);
  }

  inline bool key_cmp_less_equal(const KeyType &key1,
                                 const KeyType &key2) const {
    return !key_cmp_greater(key1, key2);
  }

  ///////////////////////////////////////////////////////////////////
  // Value Comparison Member
  ///////////////////////////////////////////////////////////////////

  inline bool value_cmp_equal(const ValueType &value1,
                              const ValueType &value2) const {
    return value_eq_obj_(value1, value2);
  }

  ///////////////////////////////////////////////////////////////////
  // Epoch management
  ///////////////////////////////////////////////////////////////////

  class EpochManager {
    // Garbage collection interval (milliseconds)
    constexpr static int GC_INTERVAL = 50;

   public:
    struct GarbageNode {
      std::unique_ptr<GarbageNode> next_ptr;
      SkipList::node *node;
    };

    struct EpochNode {
      std::atomic<int> active_thread_count;
      std::atomic<std::unique_ptr<GarbageNode>> garbage_list_ptr;
      std::unique_ptr<EpochNode> next_ptr;
    };

    using EpochNodeUniquePtr = std::unique_ptr<EpochNode>;
    using GarbageNodeUniquePtr = std::unique_ptr<GarbageNode>;

    /*
     * Constructor
     */
    EpochManager() {
      current_epoch_ptr_ = new EpochNode{};
      current_epoch_ptr_->active_thread_count = 0;
      current_epoch_ptr_->garbage_list_ptr = nullptr;
      current_epoch_ptr_->next_ptr = nullptr;

      head_epoch_ptr_ = std::move(current_epoch_ptr_);

      thread_ptr_ = nullptr;
      exited_flag_.store(false);
    }

    /*
     * Destructor
     */

    ~EpochManager() {
      // Set stop flag and let thread terminate
      exited_flag_.store(true);

      if (thread_ptr_ == nullptr) {
        LOG_TRACE("Waiting for thread");
        thread_ptr_->join();
        thread_ptr_.reset();
        LOG_TRACE("Thread Stops");
      }

      current_epoch_ptr_ = nullptr;

      ClearEpoch();

      if (head_epoch_ptr_ != nullptr) {
        for (auto epoch_node_ptr = head_epoch_ptr_.get();
             epoch_node_ptr != nullptr;
             epoch_node_ptr = epoch_node_ptr->next_ptr) {
          LOG_DEBUG("Active thread count: %d",
                    epoch_node_ptr->active_thread_count.load());
          epoch_node_ptr->active_thread_count = 0;
        }

        LOG_DEBUG("Retry cleaning");
        ClearEpoch();
      }

      PL_ASSERT(head_epoch_ptr_ == nullptr);
      LOG_TRACE("GC has finished freeing all garbage nodes");
    }

    void CreateNewEpoch() {
      EpochNodeUniquePtr epoch_node_ptr(new EpochNode{});

      epoch_node_ptr->active_thread_count = 0;
      epoch_node_ptr->garbage_list_ptr = nullptr;

      epoch_node_ptr->next_p = nullptr;

      current_epoch_ptr_->next_p = std::move(epoch_node_ptr);
      current_epoch_ptr_ = std::move(epoch_node_ptr);
    }

    inline EpochNode *JoinEpoch() {
      while (1) {
        const auto prev_count =
            current_epoch_ptr_->active_thread_count.fetch_add(1);

        if (prev_count < 0) {
          current_epoch_ptr_->active_thread_count.fetch_sub(1);
        }
        return current_epoch_ptr_.get();
      }
    }

    inline void LeaveEpoch(EpochNodeUniquePtr epoch_ptr) {
      epoch_ptr->active_thread_count.fetch_sub(1);
    }

    void ClearEpoch() {
      while (1) {
        if (head_epoch_ptr_ == current_epoch_ptr_) {
          LOG_TRACE("Current epoch is head epoch. Do not clean");
          break;
        }

        int active_thread_count = head_epoch_ptr_->active_thread_count.load();
        PL_ASSERT(active_thread_count >= 0);

        if (active_thread_count != 0) {
          LOG_TRACE("Head epoch is not empty. Return");
          break;
        }

        if (head_epoch_ptr_->active_thread_count.fetch_sub(MAX_THREAD_COUNT) >
            0) {
          LOG_TRACE(
              "Some thread sneaks in after we have decided to clean. Return");

          head_epoch_ptr_->active_thread_count.fetch_add(MAX_THREAD_COUNT);
          break;
        }

        const GarbageNodeUniquePtr next_garbage_node_ptr = nullptr;
        for (auto garbage_node_ptr = head_epoch_ptr_->garbage_list_ptr.load();
             garbage_node_ptr != nullptr;
             garbage_node_ptr = std::move(next_garbage_node_ptr)) {
          next_garbage_node_ptr = std::move(garbage_node_ptr->next_ptr);
          // TODO free the garbage node node here yo
          garbage_node_ptr.reset();
        }
        auto next_epoch_node_ptr = std::move(head_epoch_ptr_->next_ptr);
        // TODO delete the current garbage node ptr? is this needed?
        head_epoch_ptr_.reset();
        head_epoch_ptr_ = std::move(next_epoch_node_ptr);
      }
    }

    void PerformGarbageCollection() {
      ClearEpoch();
      CreateNewEpoch();
    }

   private:
    EpochNodeUniquePtr head_epoch_ptr_;
    EpochNodeUniquePtr current_epoch_ptr_;
    std::atomic<bool> exited_flag_;

    // If GC is done with external thread then this should be set
    // to nullptr
    // Otherwise it points to a thread created by EpochManager internally
    std::unique_ptr<std::thread> thread_ptr_;
  };
};

}  // namespace index
}  // namespace peloton
