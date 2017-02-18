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

  enum class NodeType : short {
    InnerNode = 0,
    LeafDataNode = 1,
    HeaderNode = 2,
    FooterNode = 3,
  };

 public:
  SkipList() :
      key_cmp_obj{KeyComparator()},
      key_eq_obj{KeyEqualityChecker()} {
    for (short i = 0; i < SKIPLIST_MAX_LEVEL; ++i) {
      BaseNode *h_node = new BaseNode(NodeType::HeaderNode, i);
      BaseNode *f_node = new BaseNode(NodeType::FooterNode, i);
      h_node->right = f_node;

      if (i > 0) {
        h_node->down = header[i - 1];
        f_node->down = footer[i - 1];
      }
      header.push_back(h_node);
      footer.push_back(f_node);
    }
  }


  /*
  * find() - Given a search key, return the leaf node with key no smaller than find key
  *          if no such key exists, return nullptr
  */
  BaseNode *find(const KeyType &find_key) {
    int current_level = SKIPLIST_MAX_LEVEL;
    BaseNode *current_node = header[SKIPLIST_MAX_LEVEL - 1];

    // loop invariant:
    // 1). current_level non-negative
    // 2). current_node is either header node or data node with key < find_key
    while (current_level >= 0) {
      if (current_node->right->GetType() == NodeType::FooterNode) {
        if (current_level == 0) {
          return nullptr;
        } else {
          current_level--;
          current_node = current_node->down;
        }
      } else {  // right node is data node

        if (key_cmp_obj(current_node->right->node_key, find_key)) {
          current_node = current_node->right;
        } else {
          if (current_level == 0) {
            return current_node->right;
          } else {
            current_level--;
            current_node = current_node->down;
          }
        }
      }
    }

    return nullptr;
  }

 private:
  std::vector<BaseNode *> header;
  std::vector<BaseNode *> footer;

  class NodeMetaData {
   public:

    NodeType node_type;
    short level;
    bool deleted;
    uint32_t epoch;
    /*
     * Constructor
     */
    NodeMetaData(NodeType node_type, short level) : node_type{node_type} {
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
    BaseNode *right;
    BaseNode *down;
    KeyType node_key;
    ValueType item_value;

    /*
     * Constructor - Initialize type and metadata
     */
    BaseNode(NodeType node_type,
             short level) : metadata{node_type, level} {
      this->right = nullptr;
      this->down = nullptr;
    };

    BaseNode(NodeType node_type,
             short level,
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
