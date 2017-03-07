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
#include "common/logger.h"
#include <sstream>
#include <strings.h>


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

      const int find_less_than = -1; // <
      const int find_equal = 0;  // ==
      const int find_greater_equal = 1;  // >=

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
               bool allowDupKey = false)
        : key_cmp_obj{p_key_cmp_obj},
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
          h_node->topLevel = SKIPLIST_MAX_LEVEL;
          BaseNode *f_node = new BaseNode(NodeType::FooterNode, i);
          f_node->topLevel = SKIPLIST_MAX_LEVEL;

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

//        std::ostringstream ss;
//        ss << std::this_thread::get_id();
//        std::string idstr = ss.str();
//
//        LOG_DEBUG("thread %s before insert\n", idstr.c_str());  // todo: need to remove this
//        printSkipListStructure();  // todo: need to remove this

        // if not support duplicate key and key exists
        if (!supportDupKey) {
          BaseNode *found = search_key(key, 0, find_equal);
          if (found != nullptr && !(found->is_deleted())) {
//            LOG_DEBUG("dup found. Refuse insertion\n");  // todo: need to remove this
            return false;
          }
        }
        else{
          // check (key,value) pair already exists
          BaseNode *found = search_key(key, 0, find_equal);
//          LOG_DEBUG("support dup key but not dup key-value pair\n");  // todo: need to remove this
          if (found != nullptr) {
            while(!(found->is_footer_node()) && KeyCmpEqual(found->node_key, key)) {
              if (ValueCmpEqual(found->item_value, value)) return false;
              found = found->get_right_node();
            }
          }
        }


        // insert from level 0 up until maxLevel
        // max level will be in range [0, 15] inclusive
        int maxLevel = generateLevel();
//        LOG_DEBUG("insert level is %d\n", maxLevel);  // todo: need to remove this
        std::vector<BaseNode *> nodes;

        // bottom up insertion, contrasting top-down deletion
        for (int i = 0; i <= maxLevel; i++) {
//          LOG_DEBUG("inserting on level %d\n", i);  // todo: need to remove this

          NodeType curType = NodeType::LeafDataNode;
          if (i > 0) {
            curType = NodeType::InnerDataNode;
          }

          // todo: get memory from mem pool
          BaseNode *node = new BaseNode(curType, i, key, value);
          node->topLevel = maxLevel;

          if (i > 0) {
            node->down = nodes[i - 1];
            (nodes[i - 1])->up = node;
          } else {
            node->down = nullptr;
          }

          nodes.push_back(node);

//          LOG_DEBUG("CAS on level %d\n", i);  // todo: need to remove this

          /*
           * on current level, start insert node. Keep trying until
           * CAS succeeds
           */
          while (true) {
            BaseNode *prev = level_find_insert_pos(key, i);
            BaseNode *ptrValue  = prev->get_right_node();

            //test code below, just checking  //todo: improve this


            if (!supportDupKey) {
              BaseNode *dada = ptrValue;
              if (!(dada->is_footer_node()) && KeyCmpEqual(dada->node_key, key) && !(dada->is_deleted())) {
                return false;
              }
            }
            else{
              // check (key,value) pair already exists
              BaseNode *dada = ptrValue;
              if (dada != nullptr) {
                while(!(dada->is_footer_node()) && KeyCmpEqual(dada->node_key, key)) {
                  if (ValueCmpEqual(dada->item_value, value) && !(dada->is_deleted())) return false;
                  dada = dada->get_right_node();
                }
              }
            }

//            LOG_DEBUG("find pos done\n");  // todo: need to remove this

//            LOG_DEBUG("get right done\n");  // todo: need to remove this
            node->right = ptrValue;
//            LOG_DEBUG("right assign done done\n");  // todo: need to remove this
            if (__sync_bool_compare_and_swap(&(prev->right), ptrValue, node)) {
//              LOG_DEBUG("CAS SUCCESS\n");  // todo: need to remove this
              break;
            }
          }
        }
//        LOG_DEBUG("thread %s after insert\n", idstr.c_str());  // todo: need to remove this
//        printSkipListStructure();  // todo: need to remove this
        return true;
      }

      bool delete_key(const KeyType &key, const ValueType &value) {

//        LOG_DEBUG("before delete\n");  // todo: need to remove this
//        printSkipListStructure();  // todo: need to remove this

        BaseNode *found = search_key(key, 0, find_equal);
        if (found == nullptr) return false;  // no such key

        while (!ValueCmpEqual(found->item_value, value)) {
          found = get_right_undeleted_node(found);
          if (found == nullptr || found->is_footer_node() || !KeyCmpEqual(found->node_key, key)) return false;
        }

        // control reaches here, then we are sure that found == node needs deleting
        assert(KeyCmpEqual(found->node_key, key));
        assert(ValueCmpEqual(found->item_value, value));

        // some other node is deleting this node as well. We accept their kindness
        if (found->is_deleted()) return true;

        found->mark_deleted();
        int topLevel = found -> topLevel;

        BaseNode *currentDeleteNode = found;

        // go to top node
        for (int i = topLevel; i > 0; i--) {
          currentDeleteNode = currentDeleteNode -> up;
          currentDeleteNode->mark_deleted();
        }

        // top down delete
        int currentLevel = topLevel;
        while (currentLevel >= 0) {
          BaseNode *prev = find_prev_node(currentDeleteNode, currentLevel);
          if (prev == nullptr) {
            // this node has been removed, meaning someone else is deleting. We can return now.
//            currentDeleteNode = currentDeleteNode -> down;
//            currentLevel --;
            return true;
          }
          else {
            if (__sync_bool_compare_and_swap(&(prev->right), currentDeleteNode, currentDeleteNode->get_right_node())) {
              currentDeleteNode = currentDeleteNode -> down;
              currentLevel --;
            }
          }
        }

//        LOG_DEBUG("after delete\n");  // todo: need to remove this
//        printSkipListStructure();  // todo: need to remove this
        return true;
      }

      void GetValue(const KeyType &key, std::vector<ValueType> &value_list) {
//        LOG_DEBUG("GetValue()");
//        printSkipListStructure();
        BaseNode *found = search_key(key, 0, find_equal);
        if (found != nullptr) {
//          LOG_DEBUG("pushing back");
          value_list.push_back(found->item_value);
          while (true) {
            BaseNode *nextNode = get_right_undeleted_node(found);
            if (nextNode != nullptr && KeyCmpEqual(nextNode->node_key, key)) {
              found = nextNode;
//              LOG_DEBUG("pushing back in while");
              value_list.push_back(found->item_value);
            } else {
              break;
            }
          }
        }
//        LOG_DEBUG("find returning with size %lu", value_list.size());
        return;
      }

      using KeyValuePair = std::pair<KeyType, ValueType>;

      BaseNode* begin() {
        BaseNode *start = header[0];
        return get_right_undeleted_node(start);
      }

      BaseNode* begin(KeyType& lowKey) {
        BaseNode *start = search_key(lowKey, 0, find_greater_equal);
        return start;
      }

      BaseNode* next(BaseNode *current) {
        return get_right_undeleted_node(current);
      }


      // This gives a hint on whether GC is needed on the index
      // For those that do not need GC this always return false
      bool NeedGC() { return false; }  // TODO: add need gc check

      // This function performs one round of GC
      // For those that do not need GC this should return immediately
      void PerformGC() { return; }  // TODO: add perform gc


      /*
         * KeyCmpLessEqual() - Compare a pair of keys for <= relation
         */
      inline bool KeyCmpLessEqual(const KeyType &key1, const KeyType &key2) const {
        return !KeyCmpGreater(key1, key2);
      }
    private:
      std::vector<BaseNode *> header;
      std::vector<BaseNode *> footer;
      bool supportDupKey;  //  whether or not support duplicate key in skiplist

      /*
       * Given a node, find the first non-footer node to its right that is not
       * deleted return null if no such node
       */
      BaseNode *get_right_undeleted_node(BaseNode *current) {
        // should never pass in a null pointer
        assert(current != nullptr);
        // a header/inner/leaf always have right pointer
        assert(current->get_right_node() != nullptr);

        BaseNode *next = current->get_right_node();

        while (next->is_deleted() && next->node_type != NodeType::FooterNode) {
          next = next->get_right_node();
        }

        if (next->node_type == NodeType::FooterNode || next->is_deleted()) {
          return nullptr;
        } else {
          return next;
        }
      }

      /*
       * search_key() - Given a key, return the node at stop_level, given the
       *                find_mode. If no such key exists, return nullptr
       *
       * If the search_mode is find_prev, then return the largest node with
       * key < search key
       * If the search_mode is find_equal, then return node with key = search key
       * If the search_mode is find_gte, then return the smallest node with
       * key >= search key
       *
       */
      BaseNode *search_key(const KeyType &key, int stop_level, int search_mode) {
        assert(stop_level >= 0 &&
               stop_level < SKIPLIST_MAX_LEVEL);  // 0~15 inclusive

        int current_level = SKIPLIST_MAX_LEVEL - 1;
        BaseNode *current_node = header[current_level];
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
              if (search_mode == find_less_than && !(current_node->is_header_node())) {
                return current_node;
              } else {
                return nullptr;
              }
            } else {
              // start search from lower level
              current_level--;
              current_node = current_node->down;
            }
          } else {

            // current right < search key, safe to move there
            if (KeyCmpLess(current_right->node_key, key)) {
              current_node = current_right;

            } else
              // current right == key
            if (KeyCmpEqual(current_right->node_key, key)) {

              if (current_level == stop_level) {
                if (search_mode == find_equal || search_mode == find_greater_equal) {
                  return current_right;
                } else {
                  return current_node->is_header_node() ? nullptr : current_node;
                }
              }
              else {
//                if (search_mode == find_equal || search_mode == find_greater_equal) {
//                  current_node = current_right;
//                  int startLevel = current_level;
//                  while (startLevel > stop_level) {
//                    current_node = current_node->down;
//                    startLevel--;
//                  }
//                  return current_node;
//                } else {
                  current_level--;
                  current_node = current_node->down;
//                }
              }
            }
              // current right larger than search key
            else {
              if (current_level == stop_level) {
                if (search_mode == find_equal) {
                  return nullptr;
                }
                if (search_mode == find_greater_equal) {
                  return current_right;
                }
                return current_node->is_header_node() ? nullptr : current_node;
              } else {
                current_level--;
                current_node = current_node->down;
              }
            }
          }
        }
        assert(false);  // control should never reach here
        return nullptr;
      }

      /*
       * Given a BaseNode node, find a BaseNode prev on level, such that
       * prev -> get_right_node() == node
       */
      BaseNode* find_prev_node(BaseNode* node, int level) {
        BaseNode *prev = search_key(node->node_key, level, find_less_than);
        if (prev == nullptr) prev = header[level];
        while (1) {
          if (prev -> get_right_node() == node) return prev;
          prev = prev -> get_right_node();
          if (prev->is_footer_node() || KeyCmpGreater(prev->node_key, node->node_key)) return nullptr;
        }
//        BaseNode *prev =  header[level];
//        while (1) {
//          if (prev->get_right_node() == node) return prev;
//          if (prev->is_footer_node()) return nullptr;
//          prev = prev->get_right_node();
//        }
      }


//      int generateLevel() { return 0; }
      int generateLevel() {
        int bound = (1 << SKIPLIST_MAX_LEVEL) - 1;
        int random = std::rand() % bound;
        return ffs(random + 1) - 1;
      }
//      int generateLevel() { return std::rand() & (SKIPLIST_MAX_LEVEL - 1); }

      /*
       * Given a key and level number, return the node in this after
       * after which this new key should be inserted
       */
      BaseNode *level_find_insert_pos(const KeyType &key, int level) {
        assert(level >= 0 && level < SKIPLIST_MAX_LEVEL);
        BaseNode *prev = search_key(key, level, find_less_than);
        if (prev == nullptr) {
          return header[level];
        } else {
          return prev;
        }
      }

      class BaseNode {
      public:
        BaseNode *right;  // the node to the right of current node
        BaseNode *down;   // node under current node. leaf node have no down node
        BaseNode *up;   // node above current node. This facilitates delete op
        KeyType node_key;
        ValueType item_value;
        NodeType node_type;
        short level;  // records the level in which this node resides
        short topLevel; // record the highest level node that will down-trace to this node

        /*
         * marks the LSB of the filed "right" with 1 to indicate this node has been
         * deleted
         */
        void mark_deleted() {
          right =
            reinterpret_cast<BaseNode *>((reinterpret_cast<uint64_t>(right)) | 0x1);
        }

        bool is_deleted() {
          return ((reinterpret_cast<uint64_t>(right)) & 0x1) == 1;
        };

        BaseNode* get_right_node() {
          return reinterpret_cast<BaseNode *>((reinterpret_cast<uint64_t>(right)) &
                                              (~0x1));
        }

        bool is_header_node() { return this->node_type == NodeType::HeaderNode; }

        bool is_footer_node() { return this->node_type == NodeType::FooterNode; }

        BaseNode(NodeType node_type, short level)
          : node_type{node_type}, level{level} {
          this->right = nullptr;
          this->down = nullptr;
          this->up = nullptr;
          this->topLevel = 0;
        };

        BaseNode(NodeType node_type, short level, KeyType node_key,
                 ValueType item_value)
          : BaseNode{node_type, level} {
          this->node_key = node_key;
          this->item_value = item_value;
          this->topLevel = 0;
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

      /////////////////////////////
      // Value Comparison Member //
      /////////////////////////////

      /*
       * ValueCmpEqual() - Compares whether two values are equal
       */
      inline bool ValueCmpEqual(const ValueType &v1, const ValueType &v2) {
        return value_eq_obj(v1, v2);
      }

      void printSkipListStructure() {
        for (int i = SKIPLIST_MAX_LEVEL - 1; i >= 0; i--) {
          BaseNode *node = header[i];
          std::string output("Level ");
          output = output + std::to_string(i) + "-->";

          node = node->get_right_node();
          while (!node->is_footer_node()) {

            BaseNode *nn = node->get_right_node();
            if ((!(nn->is_footer_node())) && KeyCmpGreater(node->node_key, nn->node_key)) {
              LOG_DEBUG("FUCKFUCKFUCKFUCKFUCKFUCKFUCKFUCKFUCKFUCKFUCKFUCKFUCK\n");
            }

            output = output + std::string("a key") + "-->";
            node = node->get_right_node();
          }

          output = output + "END";
          LOG_DEBUG("%s\n", output.c_str());
        }
      }

    };  // End skiplist class

  }  // End index namespace
}  // End peloton namespace
