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

#include <functional>
#include <vector>
#include <cstdlib>
#include <atomic>
namespace peloton {
namespace index {

/*
 * SKIPLIST_TEMPLATE_ARGUMENTS - Save some key strokes
 */
#define SKIPLIST_TEMPLATE_ARGUMENTS                                       \
  template <typename KeyType, typename ValueType, typename KeyComparator, \
            typename KeyEqualityChecker, typename ValueEqualityChecker>

#define SKIPLIST_MAX_LEVEL 16

SKIPLIST_TEMPLATE_ARGUMENTS
class SkipList {
  // TODO: Add your declarations here

  class BaseNode;
  const KeyComparator key_cmp_obj;
  const KeyEqualityChecker key_eq_obj;
  typedef std::vector<SkipList::BaseNode *> ArrayNode;
  enum class NodeType : short {
    DataNode = 0,
    HeaderNode = 1,
    FooterNode = 2,
  };

 public:
  SkipList() :
      key_cmp_obj{KeyComparator()},
      key_eq_obj{KeyEqualityChecker()} {

    for (short i = 0; i < SKIPLIST_MAX_LEVEL; ++i) {
      BaseNode *h_node = new BaseNode(NodeType::HeaderNode, i);
      BaseNode *f_node = new BaseNode(NodeType::FooterNode, i);
      h_node->right = &footer;
      header.push_back(h_node);
      footer.push_back(f_node);
    }
  }

  /*
  * find() - Given a search key, return the first leaf node with key no smaller than find key
  *          if no such key exists, return nullptr
  */
  BaseNode *find(const KeyType &find_key) {
    int current_level = SKIPLIST_MAX_LEVEL;
    ArrayNode *current_array_node = &header;
    BaseNode *current_node = current_array_node->at(SKIPLIST_MAX_LEVEL - 1);

    // loop invariant:
    // 1). current_level non-negative
    // 2). current_node is either header node or data node with key < find_key
    while (current_level >= 0) {
      if (current_node->right == &footer) {
        if (current_level == 0) {
          return nullptr;
        } else {
          current_level--;
          current_node = current_array_node->at(current_level);
        }
      } else {  // right node is data node
        BaseNode *current_node_right = current_node->right->at(current_level);
        if (KeyCmpLess(current_node_right->node_key, find_key)) {
          current_array_node = current_node->right;
          current_node = current_node_right;
        } else {
          if (current_level == 0) {
            return current_node_right;
          } else {
            current_level--;
            current_node = current_array_node->at(current_level);
          }
        }
      }
    }
    return nullptr;
  }

  bool insert(const KeyType &key, const ValueType &value) {
    unsigned int n = 0;
    while ((std::rand() % 2 == 0) && (n < SKIPLIST_MAX_LEVEL)) {
      n++;
    }
    ArrayNode *array_node_insertion = initArrayNode(key, value, n);

    // TODO: if the CAS fails, do the cleaning work
    return insert_recursive(*array_node_insertion, header, n, SKIPLIST_MAX_LEVEL - 1);
  }

 private:
  bool insert_recursive(ArrayNode &insert_array,
                        ArrayNode &cursor_array,
                        int height,
                        int level) {
    // Implemented according to pseudo-code from http://ticki.github.io/blog/skip-lists-done-right/
    BaseNode *cursor_node = cursor_array[level];
    if (cursor_node->right != &footer && KeyCmpLess(cursor_node->right->at(level)->node_key, insert_array[0])) {
      // If right isn't "overshot" (i.e. we are going to long), we go right.
      return insert_recursive(insert_array, *(cursor_node->right), height, level);
    } else {
      BaseNode *insert_node = insert_array[level];
      if (level == 0) {
        //   We're at bottom level and the right node is overshot (or footer), hence
        //   we've reached our goal, so we insert the node in between root
        //   and the node next to root.
        ArrayNode *old = cursor_node->right;
        insert_node->right = old;
        return __sync_bool_compare_and_swap(cursor_node->right, old, insert_array);
      } else {
        if (level <= height) {
          // Our level is below the height, hence we need to insert a
          // link before we go on.
          ArrayNode *old = cursor_node->right;
          cursor_node->right = &insert_array;
          insert_node->right = old;
          if (!__sync_bool_compare_and_swap(cursor_node->right, old, insert_array)) {
            return false;
          }
        }
        return insert_recursive(insert_array, cursor_array, height, level - 1);
      }
    }
  }

  ArrayNode *initArrayNode(const KeyType &key, const ValueType &value, unsigned int n) {
    // TODO: use memory pool to re-implement this function
    ArrayNode *array_node_insertion = new std::vector<BaseNode *>;
    for (short i = 0; i < n; ++i) {
      array_node_insertion->push_back(new BaseNode(NodeType::DataNode, i, key, value));
    }
    return array_node_insertion;
  }

  ///////////////////////////////////////////////////////////////////
  // Key Comparison Member Functions
  ///////////////////////////////////////////////////////////////////

  /*
   * KeyCmpLess() - Compare two keys for "less than" relation
   *
   * If key1 < key2 return true
   * If not return false
   *
   * NOTE: In older version of the implementation this might be defined
   * as the comparator to wrapped key type. However wrapped key has
   * been removed from the newest implementation, and this function
   * compares KeyType specified in template argument.
   */
  inline bool KeyCmpLess(const KeyType &key1, const KeyType &key2) const {
    return key_cmp_obj(key1, key2);
  }

  /*
   * KeyCmpEqual() - Compare a pair of keys for equality
   *
   * This functions compares keys for equality relation
   */
  inline bool KeyCmpEqual(const KeyType &key1, const KeyType &key2) const {
    return key_eq_obj(key1, key2);
  }

  /*
   * KeyCmpGreaterEqual() - Compare a pair of keys for >= relation
   *
   * It negates result of keyCmpLess()
   */
  inline bool KeyCmpGreaterEqual(const KeyType &key1,
                                 const KeyType &key2) const {
    return !KeyCmpLess(key1, key2);
  }

  /*
   * KeyCmpGreater() - Compare a pair of keys for > relation
   *
   * It flips input for keyCmpLess()
   */
  inline bool KeyCmpGreater(const KeyType &key1, const KeyType &key2) const {
    return KeyCmpLess(key2, key1);
  }

  /*
   * KeyCmpLessEqual() - Compare a pair of keys for <= relation
   */
  inline bool KeyCmpLessEqual(const KeyType &key1, const KeyType &key2) const {
    return !KeyCmpGreater(key1, key2);
  }

  ArrayNode header;
  ArrayNode footer;

  class NodeMetaData {
   public:

    NodeType node_type;
    int level;
    bool deleted;
    uint32_t epoch;
    /*
     * Constructor
     */
    NodeMetaData(NodeType node_type, int level) : node_type{node_type} {
      if (level > SKIPLIST_MAX_LEVEL) {
        this->level = SKIPLIST_MAX_LEVEL;
      }
    };
  };

  class BaseNode {
    // We hold its data structure as private to force using member functions
    // for member access
   private:
    // This holds low key, high key, next node ID, type, depth and item count
    NodeMetaData metadata;

   public:
    std::vector<BaseNode *> *right;
    // TODO: Ensure all operations for right are done efficiently
    KeyType node_key;
    ValueType item_value;

    /*
     * Constructor - Initialize type and metadata
     */
    BaseNode(NodeType node_type,
             int level) : metadata{node_type, level} {
      this->right = nullptr;
    };

    // TODO: shall we copy item_value during initialization?
    BaseNode(NodeType node_type,
             int level,
             KeyType node_key,
             ValueType item_value
    ) : BaseNode{node_type, level} {
      this->node_key = node_key;
      this->item_value = item_value;
    }

    /*
     * GetType() - Return the type of node
     *
     * This method does not allow overridding
     */
    inline NodeType GetType() const {
      return metadata.node_type;
    }

    /*
     * GetNodeMetaData() - Returns a const reference to node metadata
     *
     * Please do not override this method
     */
    inline const NodeMetaData &GetNodeMetaData() const {
      return metadata;
    }

    inline bool isDeleted() {
      return metadata.deleted;
    };

    inline void setDeleted() {
      metadata.deleted = true;
    }

  };

//
//  void PerformGarbageCollection();
//  bool Insert(const KeyType &key, const ValueType &value);
//  bool Delete(const KeyType &key, const ValueType &value);
//
//  bool ConditionalInsert(const KeyType &key,
//                         const ValueType &value,
//                         std::function<bool(const void *)> predicate,
//                         bool *predicate_satisfied);
//
//  void GetValue(const KeyType &search_key,
//                std::vector<ValueType> &value_list);
//
//  inline bool KeyCmpLessEqual(const KeyType &key1, const KeyType &key2);
//
//  SkipList(bool start_gc_thread = true,
//           KeyComparator p_key_cmp_obj = KeyComparator{},
//           KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{},
//           ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{});

  class ForwardIterator;

  ForwardIterator Begin() {
    return ForwardIterator{this};
  }

  class ForwardIterator {
   public:
    ForwardIterator(SkipList *p_tree_p);
  };
};

}  // End index namespace
}  // End peloton namespace
