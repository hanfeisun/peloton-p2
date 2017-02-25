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
#include <thread>
#include <assert.h>
#include <cstdlib>
#include <ctime>
#include "bwtree.h"

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

      class BaseNode;
      class EpochManager;

      const KeyComparator key_cmp_obj;
      const KeyEqualityChecker key_eq_obj;
      const ValueEqualityChecker value_eq_obj;

      const int find_prev = -1;
      const int find_equal = 0;
      const int find_gte = 1;

      /*
       * types of skiplist node. A leaf data node is a node at skiplist
       * level 0 (bottom level). A InnerDataNode is a node at level > 0.
       * Such nodes have only keys and pointers without data. Header and
       * Footer nodes are nodes at the start and end at each level. So every
       * data node, inner or leaf, are n between header and footer nodes.
       *
       */
      enum class NodeType : short {
        InnerDataNode = 0,
        LeafDataNode = 1,
        HeaderNode = 2,
        FooterNode = 3,
      };

    public:
      SkipList(KeyComparator p_key_cmp_obj = KeyComparator{},
               KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{},
               ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{},
               bool allowDupKey = false) :
        key_cmp_obj{p_key_cmp_obj},
        key_eq_obj{p_key_eq_obj},
        value_eq_obj{p_value_eq_obj},
        supportDupKey{allowDupKey} {

        /*
         * Initialize header and footer. Every inner data node and
         * leaf node is contained in between the header and footer
         * of the same level
         */
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
        std::srand(std::time(0));  //  seed random number generator
      }

      ~SkipList() {}

      /*
       * Insert key into skip list
       * If key exists and supportDupKey == false then return false
       * else on each level, find the desired position to insert the
       * key using CAS. repeat until insertion succeed
       */
      bool insert(const KeyType &key, const ValueType &value) {

        // if not support duplicate key and key exists
        if (!supportDupKey) {
          BaseNode *found = search_key(key, 0, find_equal);
          if ( found != nullptr && !(found->is_deleted())) return false;
        }

        // insert from level 0 up until maxLevel
        // max level will be in range [0, 15] inclusive
        int maxLevel = generateLevel();
        std::vector<BaseNode *> nodes;

        // bottom up insertion, contrasting top-down deletion
        for (int i = 0; i <= maxLevel; i++) {

          NodeType curType = i > 0 ? NodeType::InnerDataNode : NodeType :: LeafDataNode;
          BaseNode *node = new BaseNode(curType, i, key, value);  // todo: get memory from mem pool
          if (i > 0) {
            node -> down = nodes[i - 1];
          }
          else {
            node -> down = nullptr;
          }
          nodes.push_back(node);

          while (true) {
            BaseNode *prev = level_find_insert_pos(key, i);
            node -> right = prev -> get_right_node();

            uint64_t nextAdder = reinterpret_cast<uint64_t>(prev->get_right_node());
            nextAdder = nextAdder & (~0x1);  // set last bit to 0
            BaseNode *ptrValue = reinterpret_cast<BaseNode*>(nextAdder);

            if (__sync_bool_compare_and_swap(&(prev -> right), ptrValue, node)) {
              break;
            }
          }
        }
        return true;
      }

      bool delete_key(const KeyType &key, const ValueType &value) {
        return false;
      }

      void GetValue(const KeyType &key, std::vector<ValueType> &value_list) {
        BaseNode *found = search_key(key, 0, find_equal);
        if (found != nullptr) {
          value_list.push_back(found->item_value);
          while (true) {
            BaseNode *nextNode = get_right_undeleted_node(found);
            if (nextNode != nullptr && KeyCmpEqual(nextNode->node_key, key)) {
              found = nextNode;
              value_list.push_back(found->item_value);
            }
            else {
              break;
            }
          }
        }
        return;
      }

      // This gives a hint on whether GC is needed on the index
      // For those that do not need GC this always return false
      bool NeedGC() { return false;}  // TODO: add need gc check

      // This function performs one round of GC
      // For those that do not need GC this should return immediately
      void PerformGC() { return; }  // TODO: add perform gc


    private:
      std::vector<BaseNode *> header;
      std::vector<BaseNode *> footer;
      bool supportDupKey;  //  whether or not support duplicate key in skiplist

      /*
       * Given a node, find the first non-footer node to its right that is not
       * deleted return null if no such node
       */
      BaseNode* get_right_undeleted_node(BaseNode *current) {
        assert(current != nullptr);  // should never pass in a null pointer
        assert(current->get_right_node() != nullptr);  // a header/inner/leaf always have right pointer

        BaseNode *next = current->get_right_node();

        while (next->is_deleted() && next->node_type != NodeType::FooterNode) {
          next = next -> get_right_node();
        }

        if (next->node_type == NodeType::FooterNode || next->is_deleted()) {
          return nullptr;
        }
        else {
          return next;
        }
      }

      /*
       * search_key() - Given a key, return the node at stop_level, given the find_mode
       *                if no such key exists, return nullptr
       *
       * If the search_mode is find_prev, then return the largest node with key < search key
       * If the search_mode is find_equal, then return node with key = search key
       * If the search_mode is find_gte, then return the smallest node with key >= search key
       *
       */
      BaseNode* search_key(const KeyType &key, int stop_level, int search_mode) {
        assert(stop_level >= 0 && stop_level < SKIPLIST_MAX_LEVEL);  // 0~15 inclusive

        int current_level = SKIPLIST_MAX_LEVEL;
        BaseNode *current_node = header[SKIPLIST_MAX_LEVEL - 1];
        BaseNode *current_right;

        // loop invariant:
        // 1). current_level >= stop_level
        // 2). current_node is not deleted
        // 3). current_node.key < key
        while (current_level >= stop_level) {

          current_right = get_right_undeleted_node(current_node);

          // no node that is undeleted and to the right of current node
          if (current_right == nullptr) {
            //  stop_level, search ends
            if (current_level == stop_level) {
              return search_mode == find_prev && !current_node->is_header_node() ? current_node : nullptr;
            }
            else {
              current_level--;
              current_node = current_node->down;
            }
          }
          else {

            // current right < search key, safe to move there
            if (KeyCmpLess(current_right->node_key, key)) {
              current_node = current_right;

            } // end if current right < search key
            else
              // current right == key
            if (KeyCmpEqual(current_right->node_key, key)) {

              if (current_level == stop_level) {
                if (search_mode == find_equal || search_mode == find_gte) {
                  return current_right;
                }
                else { return current_node->is_header_node()? nullptr : current_node ; }
              }
              else {

                if (search_mode == find_equal || search_mode == find_gte) {
                  current_node = current_right;
                  int startLevel = current_level;
                  while (startLevel >= stop_level) { current_node = current_node->down; startLevel--;}
                  return current_node;
                }
                else{
                  current_level--;
                  current_node = current_node->down;
                }
              }
            }
              // current right larger than search key
            else {

              if (current_level == stop_level) {
                if (search_mode == find_equal) { return nullptr; }
                if (search_mode == find_gte) { return current_right; }
                return current_node->is_header_node()? nullptr : current_node;
              }
              else {
                current_level --;
                current_node = current_node->down;
              }
            }
          }
        }
        assert(false);  // control should never reach here
        return nullptr;
      }

      int generateLevel() { return std::rand() & (SKIPLIST_MAX_LEVEL - 1); }

      /*
       * Given a key and level number, return the node in this after
       * after which this new key should be inserted
       */
      BaseNode* level_find_insert_pos(const KeyType &key, int level) {

        assert(level >= 0 && level < SKIPLIST_MAX_LEVEL);
        BaseNode *prev = search_key(key, level, find_prev);
        if (prev == nullptr) {
          return header[level];
        }
        else {
          return prev;
        }
      }

      class BaseNode {
      public:
        BaseNode *right;  // the node to the right of current node
        BaseNode *down;  // node under current node. leaf node does not have down node
        KeyType node_key;
        ValueType item_value;
        NodeType node_type;
        short level;

        /*
         * marks the LSB of the filed "right" with 1 to indicate this node has been deleted
         */
        void mark_deleted() {
          right = reinterpret_cast<BaseNode*>(reinterpret_cast<uint64_t>(right) | 0x1);
        }

        bool is_deleted() {
          return (reinterpret_cast<uint64_t>(right) & 0x1) == 1;
        };

        BaseNode *get_right_node() {
          return reinterpret_cast<BaseNode*>(reinterpret_cast<uint64_t>(right) & ~0x1);
        }

        bool is_header_node() {
          return this->node_type == NodeType :: HeaderNode;
        }

        bool is_footer_node() {
          return this->node_type == NodeType :: FooterNode;
        }

        BaseNode(NodeType node_type,
                 short level) :
          node_type{node_type},
          level{level} {
          this->right = nullptr;
          this->down = nullptr;
        };

        BaseNode(NodeType node_type,
                 short level,
                 KeyType node_key,
                 ValueType item_value ) :
          BaseNode{node_type, level} {
          this->node_key = node_key;
          this->item_value = item_value;
        }

      };

      /////////////////////////////////////
      // Key Comparison Member Functions //
      /////////////////////////////////////

      /*
       * KeyCmpLess() - Compare two keys for "less than" relation
       *
       * If key1 < key2 return true
       * If not return false
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

      /////////////////////////////
      // Value Comparison Member //
      /////////////////////////////

      /*
       * ValueCmpEqual() - Compares whether two values are equal
       */
      inline bool ValueCmpEqual(const ValueType &v1, const ValueType &v2) {
        return value_eq_obj(v1, v2);
      }

    };  // End skiplist class

  }  // End index namespace
}  // End peloton namespace
