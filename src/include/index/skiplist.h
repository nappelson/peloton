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

namespace peloton {
namespace index {

/*
 * SKIPLIST_TEMPLATE_ARGUMENTS - Save some key strokes
 */
#define SKIPLIST_TEMPLATE_ARGUMENTS                                       \
  template <typename KeyType, typename ValueType, typename KeyComparator, \
            typename KeyEqualityChecker, typename ValueEqualityChecker>

// KeyType-ValueType pair
using KeyValuePair = std::pair<KeyType, ValueType>;

///////////////////////////////////////////////////////////////////
// Key Comparison Member Functions
///////////////////////////////////////////////////////////////////

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

inline bool key_cmp_less_equal(const KeyType &key1, const KeyType &key2) const {
  return !key_cmp_greater(key1, key2);
}

///////////////////////////////////////////////////////////////////
// Value Comparison Member
///////////////////////////////////////////////////////////////////

inline bool value_cmp_equal(const ValueType &value1,
                            const ValueType &value2) const {
  return value_eq_obj_(value1, value2);
}

template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker, typename ValueEqualityChecker>
class SkipList {
 public:
  explicit inline SkipList(
      bool start_gc_thread = true,
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
  KeyComparator key_cmp_obj_;
  KeyEqualityChecker key_eq_obj_;
  ValueEqualityChecker value_eq_obj_;

  unsigned int max_level_;
  node *root_;

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
  class ForwardIterator {
   private:
    node *curr_node_;

   public:
    /*
     * Default Constructor
     */
    ForwardIterator() : kv_p{nullptr} {};

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
    inline const node &operator*() { return *node_; }

    /*
     * operator -> - Return the value pointer pointed to by this iterator
     */
    inline const node *operator->() { return &*node_; }

   private:
    /*
     * Step forward one in skip list
     */
    void step_forward();
  };
};

}  // namespace index
}  // namespace peloton
