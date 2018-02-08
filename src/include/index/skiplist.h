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

namespace peloton {
namespace index {

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
      bool start_gc_thread = true,
      KeyComparator p_key_cmp_obj = KeyComparator{},
      KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{},
      ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{})
      : key_cmp_obj_{p_key_cmp_obj}, key_eq_obj_{p_key_eq_obj}, value_eq_obj_{p_value_eq_obj}
  {}


  static void test();

  const ValueType *search(const KeyType &key)
  {
    auto *cur_node = root_;
    auto level = max_level_;
    for (auto i = level; i >= 0; i--) {
      while (key_cmp_less(cur_node->next_node.at(i), key)) {
        cur_node = cur_node->next_node.at(i);
      }
    }
    cur_node = cur_node->next_node.at(i);
    if (key_cmp_equal(cur_node->key_)) {
      return cur_node->value_;
    }
    else {
      return nullptr;
    }
  }

  bool exists(const KeyType &key) const
  {
    auto level = max_level_;
    auto *cur_node = root_;

    while (cur_node != nullptr) {
      auto cur_key = cur_node->key_;
      if (key_cmp_equal(key, cur_node->key_)) {
        return true;
      }
      else if (key_cmp_greater(key, cur_node->key)) {
        if (level == 0) {
          return false;
        }
        else {
          level -= 1;
        }
      }
      else {
        cur_node = cur_node->next_node.at(level);
      }
    }
    return false;

  }


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

    // For each level, determine the width to the next step
    std::vector<int> width;

    // For each level, store the next node
    std::vector<node *> next_node;

    // Deleted bit for this node (logically removed)
    bool deleted;
  };

  struct link {
    // Width of the link between two nodes (equal to number of bottom layer
    // links being traversed)
    unsigned short width;
  };

  ///////////////////////////////////////////////////////////////////
  // Key Comparison Member Functions
  ///////////////////////////////////////////////////////////////////

  inline bool key_cmp_less(const KeyType &key1, const KeyType &key2) const {
    return key_cmp_obj_(key1, key2);
  }

  inline bool key_cmp_equal(const KeyType &key1, const KeyType &key2) const {
    return key_eq_obj_(key1, key2);
  }

  inline bool key_cmp_greater_equal(const KeyType &key1, const KeyType &key2) const {
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

  inline bool value_cmp_equal(const ValueType &value1, const ValueType &value2) const {
    return value_eq_obj_(value1, value2);
  }

 public:

  class ForwardIterator
  {
    SkipList::node * curr_node;



  };

};

}  // namespace index
}  // namespace peloton
