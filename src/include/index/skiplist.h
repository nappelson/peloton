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

#include <stdlib.h>
#include <strings.h>
#include <atomic>
#include <forward_list>
#include <memory>
#include <thread>
#include <vector>

#include "common/logger.h"
#include "common/macros.h"

namespace peloton {
namespace index {

// Forward declare EpochManager
template <typename NodeType>
class EpochManager;

// This is the value we use in epoch manager to make sure
// no thread sneaking in while GC decision is being made
#define MAX_THREAD_COUNT ((int)0x7FFFFFFF)

// Used to create edge towers, probability any tower gets this high is basically
// 0
#define MAX_TOWER_HEIGHT 32

// Used to mask pointers (only first 48 bits are used in the pointer)
#define POINTER_MASK (((intptr_t)1) << ((intptr_t)49))

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
  struct Node;

  using KeyValuePair = std::pair<KeyType, ValueType>;

  // Temporary flag for duplicate support
  bool support_duplicates_ = false;

 public:
  explicit inline SkipList(
      KeyComparator p_key_cmp_obj = KeyComparator{},
      KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{},
      ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{})
      : key_cmp_obj_{p_key_cmp_obj},
        key_eq_obj_{p_key_eq_obj},
        value_eq_obj_{p_value_eq_obj},
        epoch_manager_{} {
    max_level_ = MAX_TOWER_HEIGHT - 1;
    // seed the level generation
    srand(time(nullptr));
    // Create start and end towers
    Node *start = new Node{};
    start->next_node = {};
    start->is_edge_tower = true;
    start->inserted = true;

    Node *end = new Node{};
    end->next_node = {};
    end->is_edge_tower = true;
    end->inserted = true;

    for (int i = 0; i < MAX_TOWER_HEIGHT; i++) {
      start->next_node.push_back(end);
    }
    root_ = start;
  }

  ~SkipList() {
    // Free all KV nodes in skip list
    auto scan_itr = Begin();

    while (!scan_itr.IsEnd()) {
      // epoch_manager_.AddGarbageNode(scan_itr.GetNode());
      auto temp = scan_itr.GetNode();
      scan_itr++;
      delete temp;
    }

    PL_ASSERT(scan_itr.IsEnd() && scan_itr.GetNode() != GetRoot());
    // Free start and end towers
    epoch_manager_.AddGarbageNode(scan_itr.GetNode());
    epoch_manager_.AddGarbageNode(GetRoot());
    root_ = nullptr;
    LOG_TRACE("SkipList freed");
  }

  void SetSupportDuplicates(bool support) { support_duplicates_ = support; }

  /*
   * DoIntegrityCheck() - Checks the validity of the list by ensuring each level
   * is sorted by key. If there is a node out of place it will
   * remove the node and reorder it. Returns whether the skip list
   * was valid.
   */
  bool DoIntegrityCheck() {
    auto epoch = epoch_manager_.JoinEpoch();

    unsigned int current_level = MAX_TOWER_HEIGHT - 1;

    bool valid_structure = true;
    while (current_level > 0) {
      Node *cur_node = GetAddress(root_->next_node.at(current_level));

      while (!cur_node->is_edge_tower) {
        Node *next_node = GetAddress(cur_node->next_node.at(current_level));

        if (next_node->is_edge_tower) {
          break;
        }
        // Node out of place, need to reorder
        if (!NodeLessThanEqual(next_node->kv_p.first, cur_node)) {
          valid_structure = false;

          Remove(next_node->kv_p.first, next_node->kv_p.second);
          Insert(next_node->kv_p.first, next_node->kv_p.second);

          // Validate level again
          current_level++;
          break;
        }
        cur_node = next_node;
      }

      current_level--;
    }

    epoch_manager_.LeaveEpoch(epoch);
    return valid_structure;
  }

  /*
   * Insert() - insert a node with a given key and value into skiplist
   */
  bool Insert(const KeyType &key, const ValueType &value) {
    auto epoch = epoch_manager_.JoinEpoch();

    std::vector<Node *> parents = FindParents(key, value);

    Node *new_node = new Node{};
    new_node->is_edge_tower = false;
    new_node->kv_p = std::make_pair(key, value);
    new_node->inserted = false;

    unsigned int height = generate_level() % MAX_TOWER_HEIGHT;
    unsigned int current_level = 0;

    new_node->next_node = std::vector<Node *>(height);

    // insert node into skiplist from bottom up
    while (current_level < height) {
      Node *next_node = parents[current_level]->next_node.at(current_level);

      // parent was deleted need to find new parent
      if (IsMarked(next_node)) {
        parents[current_level] = UpdateParent(root_, current_level, key, value);
        continue;
      }

      // Key already inserted, abort operation
      if ((!support_duplicates_ && NodeEqual(key, next_node)) ||
          NodeEqual(key, value, next_node)) {
        if (current_level == 0) {
          epoch_manager_.AddGarbageNode(new_node);
          epoch_manager_.LeaveEpoch(epoch);
          return false;
        }

        // Should never get past first level if key already exists elsewhere
        PL_ASSERT(false);
      }

      // Node inserted after parent need to find new parent
      if (NodeLessThan(key, next_node)) {
        parents[current_level] =
            UpdateParent(parents[current_level], current_level, key, value);
        continue;
      }

      new_node->next_node[current_level] = next_node;

      // If CAS succeeds go to next level, otherwise try again
      if (__sync_bool_compare_and_swap(
              &(parents[current_level])->next_node[current_level], next_node,
              new_node)) {
        current_level++;
      }
    }

    // Node successfully inserted, allow the node to be deleted
    new_node->inserted = true;

    epoch_manager_.LeaveEpoch(epoch);
    return true;
  }

  /*
   * ConditionalInsert() - insert based on whether or not predicate is satisfied
   */
  bool ConditionalInsert(const KeyType &key, const ValueType &value,
                         std::function<bool(const void *)> predicate,
                         bool *predicate_satisfied) {
    auto epoch = epoch_manager_.JoinEpoch();

    // Search for node
    Node *node = FindNode(key);
    PL_ASSERT(node->is_edge_tower || key_cmp_less(node->kv_p.first, key));

    // If we start with start tower, jump to next
    if (node->is_edge_tower) {
      node = GetAddress(node->next_node[0]);
    }

    *predicate_satisfied = false;

    while (!node->is_edge_tower && key_cmp_less_equal(node->kv_p.first, key)) {
      *predicate_satisfied =
          *predicate_satisfied || predicate(node->kv_p.second);

      node = GetAddress(node->next_node[0]);
    }

    bool res = false;
    // If predicate was never satisfied, insert
    if (!(*predicate_satisfied)) {
      res = Insert(key, value);
    }

    epoch_manager_.LeaveEpoch(epoch);

    return res;
  }

  /*
   * Remove() - In order to delete without worrying about delete/insert race
   * conditions, we decided to mark the next pointers in each node
   * that is being deleted. By doing that we can check if the node
   * is being deleted and also update the pointer atomically. Additionally,
   * to ensure that we do not delete nodes that have not been fully
   * inserted we only delete nodes that have been marked successfully
   * inserted.
   */
  bool Remove(const KeyType &key, const ValueType &val) {
    auto epoch = epoch_manager_.JoinEpoch();

    std::vector<Node *> parents = FindParents(key, val);

    Node *del_node;

    // First find the node we are deleting
    do {
      del_node = parents[0]->next_node[0];

      // bottom level parent being deleted, find new parent
      while (IsMarked(del_node)) {
        parents[0] = UpdateParent(root_, 0 /* level */, key, val);
        del_node = parents[0]->next_node[0];
      }

      // found the node
      if (NodeEqual(key, val, del_node)) {
        break;
      }

      // Node does not exist, abort operation
      if (!NodeLessThan(key, del_node)) {
        epoch_manager_.LeaveEpoch(epoch);
        return false;
      }

      parents[0] = UpdateParent(parents[0], 0 /* level */, key, val);
    } while (true);

    // Node has not been fully inserted so is not available for deletion
    if (!del_node->inserted) {
      epoch_manager_.LeaveEpoch(epoch);
      return false;
    }

    int current_level = del_node->next_node.size() - 1;

    bool marked_pointer = false;

    // delete the node from the skiplist from the top down
    while (current_level >= 0) {
      Node *next_node = del_node->next_node.at(current_level);

      // node already being deleted, abort operation
      if (!marked_pointer && IsMarked(next_node)) {
        epoch_manager_.LeaveEpoch(epoch);
        return false;
      }

      // mark the next pointer so future nodes know it is being deleted
      if (!marked_pointer) {
        Node *marked_node = MaskPointer(next_node);
        if (!__sync_bool_compare_and_swap(
                &(del_node->next_node.at(current_level)), next_node,
                marked_node)) {
          continue;
        }
      }

      marked_pointer = true;

      Node *next_tmp = parents[current_level]->next_node.at(current_level);

      // parent node being deleted, find new parent
      if (IsMarked(next_tmp)) {
        parents[current_level] = UpdateParent(root_, current_level, key, val);
        continue;
      }

      // Node was deleted by another thread, abort operation
      if (NodeGreaterThan(key, next_tmp)) {
        epoch_manager_.LeaveEpoch(epoch);
        return false;
      }

      // something inserted after parent
      if (!NodeEqual(key, val, next_tmp)) {
        parents[current_level] =
            UpdateParent(next_tmp, current_level, key, val);
        continue;
      }

      next_node = UnmaskPointer(next_node);

      // CAS parents next to del_node's next
      if (__sync_bool_compare_and_swap(
              &(parents[current_level]->next_node.at(current_level)), del_node,
              next_node)) {
        current_level--;
        marked_pointer = false;
      }
    }

    // mark fully deleted node as a garbage node
    epoch_manager_.AddGarbageNode(del_node);
    epoch_manager_.LeaveEpoch(epoch);
    return true;
  }

  /*
   * UpdateParent() - Returns the last node after the current parent that is
   * still before the key
   */
  Node *UpdateParent(Node *parent, int level, const KeyType &key,
                     const ValueType &val) {
    Node *next_node = GetAddress(parent->next_node.at(level));
    while (NodeLessThan(key, val, next_node)) {
      parent = next_node;
      next_node = GetAddress(parent->next_node.at(level));
    }
    return parent;
  }

  /*
   * FindParents() - Returns a vector of Nodes which come directly before the
   * key in each level
   * first finds the nodes which come before the key, and then will increment
   * each until it is directly before the key/value pair
   */
  std::vector<Node *> FindParents(const KeyType &key, const ValueType &val) {
    // Node directly before the key in each level
    std::vector<Node *> parents(MAX_TOWER_HEIGHT);

    int current_tower = MAX_TOWER_HEIGHT - 1;
    Node *current_node = root_;
    Node *next_node;

    // First find the parent of the current Key
    // parent of one level is at least the parent of the level above
    while (current_tower >= 0) {
      next_node = GetAddress(current_node->next_node.at(current_tower));

      // not done traversing
      if (NodeLessThan(key, next_node)) {
        current_node = next_node;
        // found parent of current level
      } else {
        parents.at(current_tower) = current_node;
        current_tower--;
      }
    }

    current_tower = MAX_TOWER_HEIGHT - 1;
    current_node = parents.at(current_tower);

    // Traverse same key as long as it does not equal the key we are looking for
    while (current_tower >= 0) {
      next_node =
          GetAddress(parents.at(current_tower)->next_node.at(current_tower));

      if (NodeLessThan(key, val, next_node)) {
        parents.at(current_tower) = next_node;
      } else {
        current_tower--;
      }
    }

    return parents;
  }

  /*
   * Search() - Search for value given key
   * Returns true if found, and sets value to value, else returns false
   */
  bool Search(const KeyType &key, ValueType &value) const {
    // Join Epoch
    auto epoch_node = epoch_manager_.JoinEpoch();

    // Search for node
    Node *node = FindNode(key);

    PL_ASSERT(!IsLogicalDeleted(node));
    PL_ASSERT(!node->is_edge_tower);
    if (!key_cmp_equal(node->kv_p.first, key)) {
      // Leave Epoch
      epoch_manager_.LeaveEpoch(epoch_node);

      return false;
    }

    value = node->kv_p.second;

    // Leave Epoch
    epoch_manager_.LeaveEpoch(epoch_node);

    return true;
  }

  /*
   * Search() - Search for value given key (WITH DUPLICATES)
   * Returns true if found, and sets value to value, else returns false
   * TODO: This function may be unnecessary, but implementing just in case
   */
  bool Search(const KeyType &key, ValueType &value,
              const ValueType wanted_value) const {
    // This function should only really be used with duplicates enabled
    PL_ASSERT(support_duplicates_);

    // Join Epoch
    auto epoch_node = epoch_manager_.JoinEpoch();

    // Search for node
    Node *node = FindNode(key);
    PL_ASSERT(node->is_edge_tower || key_cmp_less(node->kv_p.first, key));

    // If we start with start tower, jump to next
    if (node->is_edge_tower) {
      node = GetAddress(node->next_node[0]);
    }

    while (!node->is_edge_tower && key_cmp_less_equal(node->kv_p.first, key)) {
      if (key_cmp_equal(node->kv_p.first, key) &&
          value_cmp_equal(node->kv_p.second, wanted_value) &&
          !IsLogicalDeleted(node)) {
        value = node->kv_p.second;

        // Leave Epoch
        epoch_manager_.LeaveEpoch(epoch_node);

        return true;
      }
      node = GetAddress(node->next_node[0]);
    }

    // Leave Epoch
    epoch_manager_.LeaveEpoch(epoch_node);

    return false;
  }

  /*
   * GetValue() - Fill a value list with values stored
   *
   * This function accepts a value list as argument,
   * and will copy all values into the list
   *
   * The return value is used to indicate whether the value set
   * is empty or not
   */
  void GetValue(const KeyType &search_key, std::vector<ValueType> &value_list) {
    auto epoch_node = epoch_manager_.JoinEpoch();

    PL_ASSERT(IsSorted());

    auto curr_node = FindNode(search_key);

    if (curr_node->is_edge_tower) {
      LOG_DEBUG("Starting scan with start tower");
    } else {
      LOG_DEBUG("Starting scan on key: %s",
                curr_node->kv_p.first.GetInfo().c_str());
    }

    if (!support_duplicates_) {
      if (!curr_node->is_edge_tower &&
          key_cmp_equal(search_key, curr_node->kv_p.first)) {
        value_list.push_back(curr_node->kv_p.second);
      }
    } else {
      PL_ASSERT(curr_node->is_edge_tower ||
                NodeLessThan(search_key, curr_node));

      curr_node = GetAddress(curr_node->next_node[0]);

      while (!curr_node->is_edge_tower &&
             key_cmp_equal(search_key, curr_node->kv_p.first)) {
        LOG_DEBUG("Adding to value_list: %s",
                  curr_node->kv_p.first.GetInfo().c_str());
        PL_ASSERT(IsSorted());
        PL_ASSERT(!key_cmp_less(curr_node->kv_p.first, search_key));
        PL_ASSERT(key_cmp_equal(search_key, curr_node->kv_p.first));
        value_list.push_back(curr_node->kv_p.second);
        curr_node = GetAddress(curr_node->next_node[0]);
      }
    }

    epoch_manager_.LeaveEpoch(epoch_node);
    return;
  }

  /*
   * FindNode() - Find node with largest key such that node.key <= key
   * If duplicates, returns largest key such that node.key < key
   */
  Node *FindNode(const KeyType &key) const {
    auto curr_tower = root_;

    LOG_DEBUG("Duplicates: %d | Looking for Key: %s", support_duplicates_,
              key.GetInfo().c_str());

    auto curr_level = MAX_TOWER_HEIGHT - 1;

    // Traverse towers, if you find it along the way, then return
    while (true) {
      PL_ASSERT((unsigned long)curr_level < curr_tower->next_node.size());
      auto next_node = GetAddress(curr_tower->next_node[curr_level]);
      while (
          (!next_node->is_edge_tower) &&  // dont jump if next node is end tower

          ((!support_duplicates_ &&
            key_cmp_greater_equal(
                key,
                next_node->kv_p.first)) ||  // jump to next node if eligible
           ((support_duplicates_ &&
             key_cmp_greater(key, next_node->kv_p.first)))) &&  // dont jump if
                                                                // node greater
                                                                // or equal to
                                                                // key

          !(key_cmp_equal(key, next_node->kv_p.first) &&
            IsLogicalDeleted(
                next_node)))  // dont jump if node you're looking for is deleted
      {
        curr_tower = next_node;
        next_node = GetAddress(curr_tower->next_node[curr_level]);
      }

      if ((!curr_tower->is_edge_tower &&
           key_cmp_equal(curr_tower->kv_p.first, key)) ||
          curr_level == 0) {
        return curr_tower;
      }
      curr_level--;
    }
  }

  /*
   * Exists() - Returns true if a key exists in skip list, false otherwise
   */
  bool Exists(const KeyType &key) const {
    ValueType dummy_value;
    return Search(key, dummy_value);
  }

  /*
   * Exists (w/ duplicates)
   */
  bool Exists(const KeyType &key, const ValueType &wanted_value) const {
    ValueType dummy_value;
    return Search(key, dummy_value, wanted_value);
  }

  /*
   * GetRoot() - Returns root of SkipList
   */
  inline Node *GetRoot() { return root_; }

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
   * GetEpochManager() - returns epoch manager for skip list (also used for
   * testing purposes)
   */
  EpochManager<Node> &GetEpochManager() { return epoch_manager_; }

  /*
   * CreateNode() - used for testing purposes only
   */
  Node *CreateNode() {
    Node *cur_node = new Node{};
    cur_node->kv_p = std::make_pair(KeyType{}, ValueType{});
    cur_node->is_edge_tower = false;
    cur_node->next_node = {};
    return cur_node;
  }

  /*
   * PrintSkipList() - Prints contents of tree
   * NOT thread or epoch safe
   */
  void PrintSkipList() {
    LOG_INFO("----- Start ToweR -----\n");
    Node *curr_node_ = GetAddress(this->GetRoot()->next_node[0]);
    while (!curr_node_->is_edge_tower) {
      if (!IsLogicalDeleted(curr_node_)) {
        LOG_INFO("Key: %s | Value %s | Height %zu",
                 curr_node_->kv_p.first.GetInfo().c_str(),
                 curr_node_->kv_p.second->GetInfo().c_str(),
                 curr_node_->next_node.size());
      }
      curr_node_ = GetAddress(curr_node_->next_node[0]);
    }
    LOG_INFO("------ End Tower ------\n");
  }

  /*
   * GetMemoryFootprint() - Returns heap footprint
   * (size of nodes in skiplist plus footprint of epoch manager)
   */
  size_t GetMemoryFootprint() {
    // If skiplist has been freed, return 0
    if (GetRoot() == nullptr) return 0;

    auto epoch_node = epoch_manager_.JoinEpoch();

    // Start at start tower
    auto curr_node = GetRoot()->next_node[0];
    size_t size = 1;

    while (!curr_node->is_edge_tower) {
      size++;
      curr_node = GetAddress(curr_node->next_node[0]);
    }

    // Add one for end tower
    size++;

    epoch_manager_.LeaveEpoch(epoch_node);

    return size * sizeof(Node) + epoch_manager_.GetMemoryFootprint();
  }

 private:
  ///////////////////////////////////////////////////////////////////
  // SkipList Helpers
  ///////////////////////////////////////////////////////////////////

  /*
   * MaskPointer() - Masks the pointer so the 49th bit is 1
   */
  inline Node *MaskPointer(Node *ptr) {
    intptr_t intp = reinterpret_cast<intptr_t>(ptr);
    intp = intp | POINTER_MASK;
    return reinterpret_cast<Node *>(intp);
  }

  /*
   * UnmaskPointer() - Unmasks the pointer so the 49th bit is 0
   */
  inline Node *UnmaskPointer(Node *ptr) {
    intptr_t intp = reinterpret_cast<intptr_t>(ptr);
    intp = intp > POINTER_MASK ? intp - POINTER_MASK : intp;
    return reinterpret_cast<Node *>(intp);
  }

  /*
   * IsLogicalDeleted() - Returns true if node is logically deleted
   */
  static inline bool IsLogicalDeleted(Node *ptr) {
    intptr_t intp = reinterpret_cast<intptr_t>(ptr);
    return (intp & POINTER_MASK) > 0;
  }

  /*
   * IsMarked() - Returns whether the pointer has been masked
   */
  inline bool IsMarked(Node *ptr) {
    intptr_t intp = reinterpret_cast<intptr_t>(ptr);
    return (intp & POINTER_MASK) > 0;
  }

  /*
   * GetAddress() - Returns address of a possibly deleted node
   */
  static inline Node *GetAddress(Node *ptr) {
    intptr_t intp = reinterpret_cast<intptr_t>(ptr);
    if (intp > POINTER_MASK) {
      return reinterpret_cast<Node *>(intp - POINTER_MASK);
    }
    return ptr;
  }

  /*
   * NodeEqual() - Returns whether the node is equal to the key
   */
  inline bool NodeEqual(KeyType key, Node *node) {
    return (!node->is_edge_tower && key_cmp_equal(node->kv_p.first, key));
  }

  /*
   * NodeEqual() - Returns whether the node is equal to the key value pair
   */
  inline bool NodeEqual(KeyType key, ValueType value, Node *node) {
    return (!node->is_edge_tower && key_cmp_equal(node->kv_p.first, key) &&
            value_cmp_equal(node->kv_p.second, value));
  }
  /*
   * NodeLessThan() - Returns true if node is not end tower and node.key < key
   * or
   * node.key = key and node.value != value (only if duplicates supported)
   */
  inline bool NodeLessThan(KeyType key, ValueType value, Node *node) const {
    return (!node->is_edge_tower &&
            (key_cmp_less(node->kv_p.first, key) ||
             (support_duplicates_ && key_cmp_equal(node->kv_p.first, key) &&
              !value_cmp_equal(node->kv_p.second, value))));
  }

  /*
   * NodeLessThan() - Returns true if node is not end tower and node.key < key
   */
  inline bool NodeLessThan(KeyType key, Node *node) const {
    return (!node->is_edge_tower && key_cmp_less(node->kv_p.first, key));
  }

  /*
   * NodeLessThanEqual() - Returns true if node is not end tower and node.key <=
   * key
   */
  inline bool NodeLessThanEqual(KeyType key, Node *node) const {
    return (!node->is_edge_tower && key_cmp_less_equal(key, node->kv_p.first));
  }

  /*
   * NodeGreaterThan() - Returns true if node is end tower or node.key > key
   */
  inline bool NodeGreaterThan(KeyType key, Node *node) const {
    return (node->is_edge_tower || key_cmp_greater(key, node->kv_p.first));
  }

  /*
   * IsSorted() - returns whether or not the skiplist is sorted
   */
  bool IsSorted() {
    auto node = GetAddress(root_);
    PL_ASSERT(node->is_edge_tower);
    node = GetAddress(node->next_node[0]);
    if (node->is_edge_tower) return true;

    while (!node->is_edge_tower) {
      auto next_node = GetAddress(node->next_node[0]);
      if (!next_node->is_edge_tower &&
          key_cmp_greater(node->kv_p.first, next_node->kv_p.first)) {
        LOG_DEBUG("FAIL - %s > %s", node->kv_p.first.GetInfo().c_str(),
                  next_node->kv_p.first.GetInfo().c_str());
        return false;
      }
      node = GetAddress(node->next_node[0]);
    }

    PL_ASSERT(node->is_edge_tower);
    LOG_DEBUG("Is sorted - True");
    return true;
  }

  struct Node {
    // Key Value pair
    KeyValuePair kv_p;

    // For each level, store the next node
    std::vector<Node *> next_node;

    // Denotes if node represents an edge tower
    bool is_edge_tower;

    // Denotes whether the node has been successfully inserted
    bool inserted;
  };

  KeyComparator key_cmp_obj_;
  KeyEqualityChecker key_eq_obj_;
  ValueEqualityChecker value_eq_obj_;

  EpochManager<Node> epoch_manager_;

  unsigned int max_level_;
  Node *root_;

  inline unsigned int generate_level() const {
    return ffs((rand() & ((1 << max_level_) - 1)));
  }

 public:
  class ForwardIterator;

  /*
   * Return iterator to beginning of skip list
   */
  ForwardIterator Begin() { return ForwardIterator{this}; }

  /*
   * Return iterator using a given key
   */
  ForwardIterator Begin(const KeyType &start_key) {
    return ForwardIterator{this, start_key};
  }

 public:
  /*
   * Iterator used for traversing all nodes of skiplist (used at base level)
   */
  class ForwardIterator {
   private:
    Node *curr_node_;
    SkipList *skip_list_;

   public:
    /*
     * Constructor given a SkipList
     */
    ForwardIterator(SkipList *skip_list) {
      // Join Epoch
      auto epoch_node = skip_list->epoch_manager_.JoinEpoch();

      curr_node_ = GetAddress(skip_list->GetRoot()->next_node[0]);
      skip_list_ = skip_list;

      // Leave epoch
      skip_list->epoch_manager_.LeaveEpoch(epoch_node);
    }

    /*
     * Constructor - Construct an iterator given a key
     *
     * The iterator returned will points to a data item whose key is greater
     * than or equal to the given start key.
     * If such key does not exist then it will
     * be the smallest key that is greater than start_key
     */
    ForwardIterator(SkipList *skip_list, const KeyType &start_key) {
      // Join Epoch
      auto epoch_node = skip_list->epoch_manager_.JoinEpoch();

      skip_list_ = skip_list;
      auto node = skip_list->FindNode(start_key);

      // If we start with start tower, jump to next
      if (node->is_edge_tower) {
        node = GetAddress(node->next_node[0]);
      }

      // Iterate until we find first node greater than or equal to key
      while (!node->is_edge_tower &&
             !skip_list->key_cmp_greater_equal(node->kv_p.first, start_key)) {
        node = GetAddress(node->next_node[0]);
      }

      PL_ASSERT(node->is_edge_tower ||
                skip_list->key_cmp_greater_equal(node->kv_p.first, start_key));

      if (node->is_edge_tower) {
        PL_ASSERT(skip_list->GetRoot() == node);
        LOG_DEBUG("Iterator starting at start tower");
      } else {
        LOG_DEBUG("Iterator starting at key: %s",
                  node->kv_p.first.GetInfo().c_str());
      }

      // set the node we found to be the current node
      curr_node_ = node;

      // Leave epoch
      // Bw_tree does it this way
      skip_list->epoch_manager_.LeaveEpoch(epoch_node);
    }

    /*
     * Destructor
     */
    ~ForwardIterator() { return; }

    /*
     * IsEnd() - True if End of Iterator
     */
    inline bool IsEnd() {
      if (curr_node_ == nullptr) {
        return true;
      }
      return curr_node_->is_edge_tower && curr_node_ != skip_list_->GetRoot();
    }

    inline Node *GetNode() { return curr_node_; }

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
    inline ForwardIterator &operator++() {
      if (IsEnd()) {
        return *this;
      } else {
        StepForward();
        return *this;
      }
    }

    /*
     * Prefix operator--
     */
    inline ForwardIterator &operator--();

    /*
     * Postfix operator++
     */
    inline ForwardIterator *operator++(int) {
      if (IsEnd()) {
        return this;
      } else {
        auto temp = this;
        StepForward();
        return temp;
      }
    }

    /*
     * Postfix operator--
     */
    inline ForwardIterator operator--(int);

    /*
     * operator * - Return the value reference currently pointed to by this
     * iterator
     */
    inline const KeyValuePair &operator*() { return &(curr_node_->kv_p); }

    /*
     * operator -> - Return the value pointer pointed to by this iterator
     */
    inline const KeyValuePair *operator->() { return &curr_node_->kv_p; }

   private:
    /*
     * Step forward one in skip list
     */
    void StepForward() {
      PL_ASSERT(!curr_node_->is_edge_tower);

      curr_node_ = GetAddress(curr_node_->next_node[0]);

      // Iterate until we find a non-deleted node
      while (!curr_node_->is_edge_tower && IsLogicalDeleted(curr_node_)) {
        curr_node_ = GetAddress(curr_node_->next_node[0]);
      }

      return;
    }
  };

  ///////////////////////////////////////////////////////////////////
  // Key Comparison Member Functions
  ///////////////////////////////////////////////////////////////////
 public:
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
};
///////////////////////////////////////////////////////////////////
// Epoch management
///////////////////////////////////////////////////////////////////

template <typename NodeType>
class EpochManager {
  // Garbage collection interval (milliseconds)
  constexpr static int GC_INTERVAL = 50;

 public:
  /*
   * GarbageNode - node in a linked list of deleted nodes
   * waiting to be GCed
   */
  struct GarbageNode {
    GarbageNode *next_ptr;
    const NodeType *node_ptr;
  };

  /*
   * EpochNode - node in linked list representing the list of
   * epochs
   */
  struct EpochNode {
    std::atomic<int> active_thread_count;
    std::atomic<GarbageNode *> garbage_list_ptr;
  };

  /*
   * PrintEpochNodeList() - Prints out information regarding the current epoch
   * node list
   */
  void PrintEpochNodeList() {
    auto count = 1;
    for (auto node_itr = epoch_node_list_.begin();
         node_itr != epoch_node_list_.end(); node_itr++) {
      auto current_node = *node_itr;
      LOG_INFO("\n Epoch Node: %d\n Pointer: %p\n Thread Count: %d", count,
               (void *)current_node, current_node->active_thread_count.load());
      count++;
    }
  }

  /*
   * GetMemoryFootprint() - Returns heap footprint of EpochManager
   * (size of nodes in garbage lists +
   * the sizes of the epoch nodes and garbage nodes)
   */
  size_t GetMemoryFootprint() {
    auto epoch_node_count = 0;
    auto garbage_node_count = 0;
    for (auto node_itr = epoch_node_list_.begin();
         node_itr != epoch_node_list_.end(); node_itr++) {
      auto current_node = *node_itr;
      epoch_node_count++;

      GarbageNode *next_garbage_node_ptr = nullptr;
      for (auto garbage_node_ptr = current_node->garbage_list_ptr.load();
           garbage_node_ptr != nullptr;
           garbage_node_ptr = next_garbage_node_ptr) {
        next_garbage_node_ptr = garbage_node_ptr->next_ptr;
        garbage_node_count++;
      }
    }
    return epoch_node_count * sizeof(EpochNode) +
           garbage_node_count * sizeof(GarbageNode) +
           garbage_node_count * sizeof(NodeType);
  }

  /*
   * GetEpochNodeList() - returns the current epoch node list
   */
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

    exited_flag_.store(false);
  }

  /*
   * Destructor
   */
  ~EpochManager() {
    // Set stop flag and let thread terminate
    exited_flag_.store(true);

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
    LOG_DEBUG("GC has finished freeing all garbage nodes");
  }

  /*
   * CreateNewEpoch() - Creates a new epoch node and increments the current
   * epoch
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

      const auto prev_count = cur_epoch_node->active_thread_count.fetch_add(1);

      if (prev_count < 0) {
        cur_epoch_node->active_thread_count.fetch_sub(1);
      }
      return *cur_epoch_node_itr_;
    }
  }

  /*
   * AddGarbageNode() - Adds a node to the current epoch's garbage list
   */
  void AddGarbageNode(const NodeType *node_ptr) {
    while (1) {
      auto cur_epoch_node = *cur_epoch_node_itr_;
      GarbageNode *garbage_node_ptr = new GarbageNode;
      garbage_node_ptr->node_ptr = node_ptr;
      garbage_node_ptr->next_ptr = cur_epoch_node->garbage_list_ptr.load();
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
   * LeaveEpoch() - Decrements epoch thread count by 1
   */
  inline void LeaveEpoch(EpochNode *epoch_ptr) {
    epoch_ptr->active_thread_count.fetch_sub(1);
  }

  /*
   * ClearEpoch() - Frees memory for all deleted nodes in the head epoch
   * (if the head epoch isn't the current epoch)
   */
  void ClearEpoch() {
    LOG_DEBUG("Clearing Epoch");
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
        delete garbage_node_ptr->node_ptr;
        delete garbage_node_ptr;
      }
      auto head_epoch_node_ptr = epoch_node_list_.front();
      epoch_node_list_.remove(head_epoch_node_ptr);
      delete head_epoch_node_ptr;
    }
  }

  /*
   * PerformGarbageCollection() - First creates a new epoch and then GCs all
   * nodes in the head epoch
   */
  void PerformGarbageCollection() {
    CreateNewEpoch();
    ClearEpoch();
  }

 private:
  std::forward_list<EpochNode *> epoch_node_list_;
  typename std::forward_list<EpochNode *>::iterator cur_epoch_node_itr_;
  std::atomic<bool> exited_flag_;
};

}  // namespace index
}  // namespace peloton
