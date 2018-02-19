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
#include <forward_list>
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
 private:
  // Forward Declarations
  class EpochManager;
  struct Node;

 public:
  explicit inline SkipList(
      KeyComparator p_key_cmp_obj = KeyComparator{},
      KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{},
      ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{})
      : key_cmp_obj_{p_key_cmp_obj},
        key_eq_obj_{p_key_eq_obj},
        value_eq_obj_{p_value_eq_obj},
        epoch_manager_{} {}

  /*
   * Insert
   */
  bool Insert(const KeyType &key, const ValueType &value);

  /*
   * Delete
   */
  bool Remove(const KeyType &key);

  /*
   * Search
   */
  const ValueType *Search(const KeyType &key) const;

  /*
   * Exists
   */
  bool Exists(const KeyType &key) const;

  /*
   * NeedGarbageCollection() - Whether the skiplist needs gaarbage collection or
   * not
   */
  bool NeedGarbageCollection() { return true; }

  /*
   * PerformGarbageCollection() - Interface to epoch manager's garbage
   * collection method
   */
  void PerformGarbageCollection() { epoch_manager_.PerformGarbageCollection(); }

  /*
   * GetEpochManager()
   */
  EpochManager &GetEpochManager() { return epoch_manager_; }

  /*
   * CreateNode() TODO: remove (only used for testing purposes right now)
   */
  Node *CreateNode() {
    Node *cur_node = new Node{};
    cur_node->key = KeyType{};
    cur_node->value = ValueType{};
    cur_node->deleted = false;
    return cur_node;
  }

 private:
  struct Node {
    // The key
    KeyType key;

    // The value
    ValueType value;

    // For each level, store the next node
    std::vector<Node *> next_node;

    // Deleted bit for this node (logically removed)
    // TODO (steal last bit from next pointer to see deleted bit)
    bool deleted;
  };

  KeyComparator key_cmp_obj_;
  KeyEqualityChecker key_eq_obj_;
  ValueEqualityChecker value_eq_obj_;

  EpochManager epoch_manager_;

  unsigned int max_level_;
  Node *root_;

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
    Node *curr_node_;

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
    inline const Node &operator*() { return *curr_node_; }

    /*
     * operator -> - Return the value pointer pointed to by this iterator
     */
    inline const Node *operator->() { return &*curr_node_; }

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
      GarbageNode *next_ptr;
      const SkipList::Node *node_ptr;
    };

    struct EpochNode {
      std::atomic<int> active_thread_count;
      std::atomic<GarbageNode *> garbage_list_ptr;
    };

    void PrintEpochNodeList() {
      auto count = 1;
      for (auto node_itr = epoch_node_list_.begin();
           node_itr != epoch_node_list_.end(); node_itr++) {
        auto current_node = *node_itr;
        LOG_INFO("\n Epoch Node: %d\n Pointer: %p\n Thread Count: %d", count,
                 (void *)current_node,
                 current_node->active_thread_count.load());
        count++;
      }
    }

    std::forward_list<EpochNode *> &GetEpochNodeList() {
      return epoch_node_list_;
    }

    EpochNode *GetCurrentEpochNode() { return *cur_epoch_node_itr_; }

    /*
     * Constructor
     */
    EpochManager() {
      // Construct the first epoch node
      auto epoch_node = new EpochNode{};
      epoch_node->active_thread_count = 0;
      epoch_node->garbage_list_ptr = nullptr;

      // Add node to the head of epoch node list
      epoch_node_list_.push_front(epoch_node);
      cur_epoch_node_itr_ = epoch_node_list_.begin();

      thread_ptr_ = nullptr;
      exited_flag_.store(false);
    }

    /*
     * Destructor
     */

    ~EpochManager() {
      // Set stop flag and let thread terminate
      exited_flag_.store(true);

      if (thread_ptr_ != nullptr) {
        LOG_TRACE("Waiting for thread");
        thread_ptr_->join();
        thread_ptr_.reset();
        LOG_TRACE("Thread Stops");
      }

      cur_epoch_node_itr_ = epoch_node_list_.end();

      ClearEpoch();

      if (!epoch_node_list_.empty()) {
        for (auto epoch_node_itr = epoch_node_list_.begin();
             epoch_node_itr != epoch_node_list_.end(); epoch_node_itr++) {
          auto epoch_node_ptr = *epoch_node_itr;
          LOG_DEBUG("Active thread count: %d",
                    epoch_node_ptr->active_thread_count.load());

          epoch_node_ptr->active_thread_count.load();
          epoch_node_ptr->active_thread_count = 0;
        }

        // TODO do we really need to do this?
        LOG_DEBUG("Retry cleaning");
        ClearEpoch();
      }

      PL_ASSERT(epoch_node_list_.empty());
      LOG_TRACE("GC has finished freeing all garbage nodes");
    }

    /*
     * CreateNewEpoch()
     */
    void CreateNewEpoch() {
      auto epoch_node_ptr = new EpochNode{};
      LOG_TRACE("created epoch node ptr");

      epoch_node_ptr->active_thread_count = 0;
      epoch_node_ptr->garbage_list_ptr = nullptr;

      LOG_TRACE("Initialized epoch node");

      epoch_node_list_.insert_after(cur_epoch_node_itr_, epoch_node_ptr);
      cur_epoch_node_itr_++;

      LOG_TRACE("Set current epoch node to new epoch");
    }

    /*
     * JoinEpoch() - Let the current thread join this epoch
     */
    inline EpochNode *JoinEpoch() {
      // We must make sure the epoch we join and the epoch we
      // return are the same one because the current point
      // could change in the middle of this function
      while (1) {
        auto &cur_epoch_node = *cur_epoch_node_itr_;

        const auto prev_count =
            cur_epoch_node->active_thread_count.fetch_add(1);

        if (prev_count < 0) {
          cur_epoch_node->active_thread_count.fetch_sub(1);
        }
        return *cur_epoch_node_itr_;
      }
    }

    /*
     * AddGarbageNode()
     */
    void AddGarbageNode(const Node *node_ptr) {
      auto cur_epoch_node = *cur_epoch_node_itr_;
      GarbageNode *garbage_node_ptr = new GarbageNode;
      garbage_node_ptr->node_ptr = node_ptr;
      garbage_node_ptr->next_ptr = cur_epoch_node->garbage_list_ptr.load();

      while (1) {
        bool ret = cur_epoch_node->garbage_list_ptr.compare_exchange_strong(
            garbage_node_ptr->next_ptr, garbage_node_ptr);

        if (ret) {
          break;
        } else {
          LOG_TRACE("Add garbage node CAS failed. Retry");
        }
      }
    }

    /*
     * LeaveEpoch()
     */
    inline void LeaveEpoch(EpochNode *epoch_ptr) {
      epoch_ptr->active_thread_count.fetch_sub(1);
    }

    /*
     * ClearEpoch()
     */
    void ClearEpoch() {
      while (1) {
        if (epoch_node_list_.begin() == cur_epoch_node_itr_) {
          LOG_TRACE("Current epoch is head epoch. Do not clean");
          break;
        }

        int active_thread_count =
            epoch_node_list_.front()->active_thread_count.load();
        PL_ASSERT(active_thread_count >= 0);

        if (active_thread_count != 0) {
          LOG_TRACE("Head epoch is not empty. Return");
          break;
        }

        if (epoch_node_list_.front()->active_thread_count.fetch_sub(
                MAX_THREAD_COUNT) > 0) {
          LOG_TRACE(
              "Some thread sneaks in after we have decided to clean. Return");

          epoch_node_list_.front()->active_thread_count.fetch_add(
              MAX_THREAD_COUNT);
          break;
        }

        GarbageNode *next_garbage_node_ptr = nullptr;
        for (auto garbage_node_ptr =
                 epoch_node_list_.front()->garbage_list_ptr.load();
             garbage_node_ptr != nullptr;
             garbage_node_ptr = next_garbage_node_ptr) {
          next_garbage_node_ptr = garbage_node_ptr->next_ptr;
          delete garbage_node_ptr;
        }
        auto head_epoch_node_ptr = epoch_node_list_.front();
        epoch_node_list_.remove(head_epoch_node_ptr);
        delete head_epoch_node_ptr;
      }
    }

    /*
     * PerformGarbageCollection()
     */
    void PerformGarbageCollection() {
      ClearEpoch();
      CreateNewEpoch();
    }

    /*
     * ThreadFunc()
     * TODO: not sure if we need this if GC is invoked for us
     */
    void ThreadFunc() {
      while (!exited_flag_.load()) {
        PerformGarbageCollection();

        // Sleep
        std::chrono::milliseconds duration(GC_INTERVAL);
        std::this_thread::sleep_for(duration);
      }
      LOG_TRACE("exit flag is true; thread return");
    }

    /*
     * StartThread()
     * TODO: not sure if we need this if GC is invoked for us
     */
    void StartThread() {
      thread_ptr_ = new std::thread{[this]() { this->ThreadFunc(); }};
    }

   private:
    std::forward_list<EpochNode *> epoch_node_list_;
    typename std::forward_list<EpochNode *>::iterator cur_epoch_node_itr_;
    std::atomic<bool> exited_flag_;

    // If GC is done with external thread then this should be set
    // to nullptr
    // Otherwise it points to a thread created by EpochManager internally
    // TODO: may not need this if GC is invoked for us
    std::unique_ptr<std::thread> thread_ptr_;
  };
};

}  // namespace index
}  // namespace peloton
