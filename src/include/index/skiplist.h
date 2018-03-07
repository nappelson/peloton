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

// Key Value Pair

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

  void SetSupportDuplicates(bool support) { support_duplicates_ = support; }

  /*
   * Insert
   */
  bool Insert(const KeyType &key, const ValueType &value) {
    auto epoch = epoch_manager_.JoinEpoch();

    PrintSkipList();

    std::vector<Node *> parents = FindParents(key, value);

    Node *new_node = new Node{};
    new_node->is_edge_tower = false;
    new_node->kv_p = std::make_pair(key, value);
    new_node->inserted = false;

    unsigned int height = generate_level() % MAX_TOWER_HEIGHT;
    unsigned int current_level = 0;

    new_node->next_node = std::vector<Node *>(height);

    while (current_level < height) {
      Node *next_node = parents[current_level]->next_node.at(current_level);

      // parent was deleted
      if (IsMarked(next_node)) {
        parents[current_level] = UpdateParent(root_, current_level, key, value);
        continue;
      }

      // Key already inserted
      if (!new_node->is_edge_tower && key_cmp_equal(key, next_node->kv_p.first) &&
          (!support_duplicates_ || value_cmp_equal(value, next_node->kv_p.second))) {
        if (current_level == 0) {
          epoch_manager_.LeaveEpoch(epoch);
          return false;
        }

        // Should never get past first level if key already exists elsewhere
        PL_ASSERT(false);
      }

      // Node inserted after parent
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

    // allow the node to be deleted
    new_node->inserted = true;

    epoch_manager_.LeaveEpoch(epoch);
    return true;
  }

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

  bool Remove(const KeyType &key) { return Remove(key, nullptr); }

  /*
   * Delete
   */
  // TODO: fix delete insert (same node) race condition
  // TODO: Add a check for attempting to delete a node that is not in the index
  bool Remove(const KeyType &key, const ValueType &val) {
    auto epoch = epoch_manager_.JoinEpoch();

    std::vector<Node *> parents = FindParents(key, val);

    Node *del_node;

    // Need to find the node we are deleting
    do {
      del_node = parents[0]->next_node[0];

      // bottom level parent being deleted
      while (IsMarked(del_node)) {
        parents[0] = UpdateParent(root_, 0 /* level */, key, val);
        del_node = parents[0]->next_node[0];
      }

      // found the node
      if (NodeEqual(key, val, del_node)) {
        break;
      }

      // Node does not exist
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

    while (current_level >= 0) {
      Node *next_node = del_node->next_node.at(current_level);

      // node already being deleted
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

      // parent node being deleted
      if (IsMarked(next_tmp)) {
        parents[current_level] = UpdateParent(root_, current_level, key, val);
        continue;
      }

      // Node was deleted
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

  // Returns the last node after the current parent that is still before the key
  Node *UpdateParent(Node *parent, int level, const KeyType &key,
                     const ValueType &val) {
    Node *next_node = GetAddress(parent->next_node.at(level));
    while (NodeLessThan(key, val, next_node)) {
      parent = next_node;
      next_node = GetAddress(parent->next_node.at(level));
    }
    return parent;
  }

  // returns a vector of Nodes which come directly before the key in each level
  // first finds the nodes which come before the key, and then will increment
  // each until it is directly before the key/value pair
  std::vector<Node *> FindParents(const KeyType &key, const ValueType &val) {
    // Node directly before the key in each level
    std::vector<Node *> parents(MAX_TOWER_HEIGHT);

    int current_tower = MAX_TOWER_HEIGHT - 1;
    Node *current_node = root_;
    Node *next_node;

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
   * Search for value given key
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
  * Search for value given key (WITH DUPLICATES)
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

    PrintSkipList();
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
   * Find node with largest key such that node.key <= key
   * If duplicates, returns largest key such that node.key < key
   * TODO: Check correctness of inner while loop
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
      //      if (curr_tower->is_edge_tower) {
      //        LOG_DEBUG("On start tower | level = %d | search_key = %s",
      //        curr_level, key.GetInfo().c_str());
      //      } else {
      //        LOG_DEBUG("On tower = %s | level = %d | search_key = %s",
      //        curr_tower->kv_p.first.GetInfo().c_str(), curr_level,
      //        key.GetInfo().c_str());
      //      }
      curr_level--;
    }
  }

  /*
   * Exists
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
   * Returns root of SkipList
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
   * GetEpochManager()
   */
  EpochManager<Node> &GetEpochManager() { return epoch_manager_; }

  /*
   * CreateNode() TODO: remove (only used for testing purposes right now)
   */
  Node *CreateNode() {
    Node *cur_node = new Node{};
    cur_node->kv_p = std::make_pair(KeyType{}, ValueType{});
    cur_node->is_edge_tower = false;
    cur_node->next_node = {};
    return cur_node;
  }


  /*
 * Prints contents of tree
 * NOT thread or epoch safe
 */
  void PrintSkipList() {
    LOG_DEBUG("----- Start Tower -----\n");
    Node *curr_node_ = GetAddress(this->GetRoot()->next_node[0]);
    while (!curr_node_->is_edge_tower) {
      if (!IsLogicalDeleted(curr_node_)) {
        LOG_DEBUG("Key: %s | Height %zu",
                  curr_node_->kv_p.first.GetInfo().c_str(),
                  curr_node_->next_node.size());
      }
      curr_node_ = GetAddress(curr_node_->next_node[0]);
    }
    LOG_DEBUG("------ End Tower ------\n");
  }

 private:
  ///////////////////////////////////////////////////////////////////
  // SkipList Helpers
  ///////////////////////////////////////////////////////////////////

  /*
   * Masks the pointer so the 49th bit is 1
   */
  inline Node *MaskPointer(Node *ptr) {
    intptr_t intp = reinterpret_cast<intptr_t>(ptr);
    intp = intp | POINTER_MASK;
    return reinterpret_cast<Node *>(intp);
  }

  /*
   * Unmasks the pointer so the 49th bit is 0
   */
  inline Node *UnmaskPointer(Node *ptr) {
    intptr_t intp = reinterpret_cast<intptr_t>(ptr);
    intp = intp > POINTER_MASK ? intp - POINTER_MASK : intp;
    return reinterpret_cast<Node *>(intp);
  }

  // Returns true if node is logically deleted
  static inline bool IsLogicalDeleted(Node *ptr) {
    intptr_t intp = reinterpret_cast<intptr_t>(ptr);
    return (intp & POINTER_MASK) > 0;
  }

  /*
   * Returns whether the pointer has been masked
   */
  inline bool IsMarked(Node *ptr) {
    intptr_t intp = reinterpret_cast<intptr_t>(ptr);
    return (intp & POINTER_MASK) > 0;
  }

  // Returns address of a possibly deleted node
  static inline Node *GetAddress(Node *ptr) {
    intptr_t intp = reinterpret_cast<intptr_t>(ptr);
    if (intp > POINTER_MASK) {
      return reinterpret_cast<Node *>(intp - POINTER_MASK);
    }
    return ptr;
  }

  /*
   * Returns whether the node is equal to the key value pair
   */
  inline bool NodeEqual(KeyType key, ValueType value, Node *node) {
    return (!node->is_edge_tower &&
            key_cmp_equal(node->kv_p.first, key) &&
            value_cmp_equal(node->kv_p.second, value));
  }

  /*
   * Returns true if node is not end tower and node.key < key or
   * node.key = key and node.value != value (only if duplicates supported)
   */
  inline bool NodeLessThan(KeyType key, ValueType value, Node *node) const {
    return (!node->is_edge_tower &&
            (key_cmp_less(node->kv_p.first, key) ||
             (support_duplicates_ && key_cmp_equal(node->kv_p.first, key) &&
              !value_cmp_equal(node->kv_p.second, value))));
  }

  /*
   * Returns true if node is not end tower and node.key < key
   */
  inline bool NodeLessThan(KeyType key, Node *node) const {
    return (!node->is_edge_tower && key_cmp_less(node->kv_p.first, key));
  }

  /*
   * Returns true if node is not end tower and node.key <= key
   */
  inline bool NodeLessThanEqual(KeyType key, Node *node) const {
    return (!node->is_edge_tower && key_cmp_less_equal(key, node->kv_p.first));
  }

  /*
   * Returns true if node is end tower or node.key > key
   */
  inline bool NodeGreaterThan(KeyType key, Node *node) const {
    return (node->is_edge_tower || key_cmp_greater(key, node->kv_p.first));
  }



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

   public:
    /*
     * Default Constructor
     * TODO: May not be needed
     */
    // ForwardIterator(){};

    /*
     * Constructor given a SkipList
     */
    ForwardIterator(SkipList *skip_list) {
      // Join Epoch
      auto epoch_node = skip_list->epoch_manager_.JoinEpoch();

      curr_node_ = GetAddress(skip_list->GetRoot()->next_node[0]);

      // Leave epoch
      skip_list->epoch_manager_.LeaveEpoch(epoch_node);
    }

    /*
     * Constructor - Construct an iterator given a key
     *
     * The iterator returned will points to a data item whose key is greater
     *than
     * or equal to the given start key. If such key does not exist then it will
     * be the smallest key that is greater than start_key
     */
    ForwardIterator(SkipList *skip_list, const KeyType &start_key) {
      // Join Epoch
      auto epoch_node = skip_list->epoch_manager_.JoinEpoch();

      auto node = skip_list->FindNode(start_key);

      // If we start with start tower, jump to next
      if (node->is_edge_tower) {
        node = GetAddress(node->next_node[0]);
      }

      // Iterate until we find first node greater than or equal to key
      while (!node->is_edge_tower && !skip_list->key_cmp_greater_equal(node->kv_p.first, start_key)) {
        node = GetAddress(node->next_node[0]);
      }

      PL_ASSERT(node->is_edge_tower ||
                skip_list->key_cmp_greater_equal(node->kv_p.first, start_key));

      // Leave epoch
      // TODO: Why shouldnt we leave the epoch upon destruction?
      // Bw_tree does it this way
      skip_list->epoch_manager_.LeaveEpoch(epoch_node);
    }

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
     * TODO: Why shouldnt we leave the epoch upon destruction?
    */
    ~ForwardIterator() { return; }

    /*
     * True if End of Iterator
     */
    inline bool IsEnd() { return curr_node_->is_edge_tower; }

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
        step_forward();
        return *this;
      }
    }

    /*
     * Prefix operator--
     */
    inline ForwardIterator &operator--();

    /*
     * Postfix operator++
     * TODO: Maybe remove the '*', i was getting a linker error without it
     */
    inline ForwardIterator *operator++(int) {
      if (IsEnd()) {
        return this;
      } else {
        auto temp = this;
        step_forward();
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
    void step_forward() {
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
  struct GarbageNode {
    GarbageNode *next_ptr;
    const NodeType *node_ptr;
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
               (void *)current_node, current_node->active_thread_count.load());
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

      const auto prev_count = cur_epoch_node->active_thread_count.fetch_add(1);

      if (prev_count < 0) {
        cur_epoch_node->active_thread_count.fetch_sub(1);
      }
      return *cur_epoch_node_itr_;
    }
  }

  /*
   * AddGarbageNode()
   */
  void AddGarbageNode(const NodeType *node_ptr) {
    // TODO: Shouldn't this code be inside the while(1)
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

}  // namespace index
}  // namespace peloton
