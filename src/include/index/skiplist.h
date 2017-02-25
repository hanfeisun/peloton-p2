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
#include "bwtree.h"


namespace peloton {
namespace index {

#define skplist_printf(fmt, ...)   \
  do {                         \
    dummy(fmt, ##__VA_ARGS__); \
  } while (0);

/*
 * SKIPLIST_TEMPLATE_ARGUMENTS - Save some key strokes
 */
#define SKIPLIST_TEMPLATE_ARGUMENTS                                       \
  template <typename KeyType, typename ValueType, typename KeyComparator, \
            typename KeyEqualityChecker, typename ValueEqualityChecker>

#define SKIPLIST_MAX_LEVEL 16

typedef peloton::index::BwTreeBase SkipListBase;

SKIPLIST_TEMPLATE_ARGUMENTS
class SkipList: public SkipListBase {
  // TODO: Add your declarations here

  class BaseNode;
  class EpochManager;
  // Key comparator
  const KeyComparator key_cmp_obj;

  // Raw key eq checker
  const KeyEqualityChecker key_eq_obj;

  // Check whether values are equivalent
  const ValueEqualityChecker value_eq_obj;
  EpochManager epoch_manager;

  enum class NodeType : short {
    InnerDataNode = 0,
    LeafDataNode = 1,
    HeaderNode = 2,
    FooterNode = 3,
  };

 public:
  SkipList(bool start_gc_thread = true,
           KeyComparator p_key_cmp_obj = KeyComparator{},
           KeyEqualityChecker p_key_eq_obj = KeyEqualityChecker{},
           ValueEqualityChecker p_value_eq_obj = ValueEqualityChecker{}) :
      SkipListBase(),
      key_cmp_obj{p_key_cmp_obj},
      key_eq_obj{p_key_eq_obj},
      value_eq_obj(p_value_eq_obj),
      epoch_manager(this) {
    initNodeLayout();
    if (start_gc_thread) {
      skplist_printf("Starting epoch manager thread...\n");
      epoch_manager.StartThread();
    }

  }

  void initNodeLayout() {
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

  ~SkipList();

  void PerformGarbageCollection();
  bool Insert(const KeyType &key, const ValueType &value);
  bool Delete(const KeyType &key, const ValueType &value);

  bool ConditionalInsert(const KeyType &key,
                         const ValueType &value,
                         std::function<bool(const void *)> predicate,
                         bool *predicate_satisfied);

  void GetValue(const KeyType &search_key,
                std::vector<ValueType> &value_list);
  void AddGarbageNode(const BaseNode *node_p);


  /*
  * find() - Given a search key, return the leaf node with key no smaller than find key
  *          if no such key exists, return nullptr
  */
//  BaseNode *find(const KeyType &find_key) {
//    int current_level = SKIPLIST_MAX_LEVEL;
//    BaseNode *current_node = header[SKIPLIST_MAX_LEVEL - 1];
//
//    // loop invariant:
//    // 1). current_level non-negative
//    // 2). current_node is either header node or data node with key < find_key
//    while (current_level >= 0) {
//      if (current_node->right->GetType() == NodeType::FooterNode) {
//        if (current_level == 0) {
//          return nullptr;
//        } else {
//          current_level--;
//          current_node = current_node->down;
//        }
//      } else {  // right node is data node
//
//        if (key_cmp_obj(current_node->right->node_key, find_key)) {
//          current_node = current_node->right;
//        } else {
//          if (current_level == 0) {
//            return current_node->right;
//          } else {
//            current_level--;
//            current_node = current_node->down;
//          }
//        }
//      }
//    }
//
//    return nullptr;
//  }

 private:
  std::vector<BaseNode *> header;
  std::vector<BaseNode *> footer;

  class BaseNode {
    // We hold its data structure as private to force using member functions
    // for member access
   private:

   public:

    /*
     * class EpochManager - Maintains a linked list of deleted nodes
     *                      for threads to access until all threads
     *                      entering epochs before the deletion of
     *                      nodes have exited
     */

   public:
    BaseNode *right;
    BaseNode *down;
    KeyType node_key;
    ValueType item_value;
    NodeType node_type;
    short level;
    uint32_t epoch;
    void mark_deleted();
    bool is_deleted();
    BaseNode *get_right_node();

    /*
     * Constructor - Initialize type and metadata
     */
    BaseNode(NodeType node_type,
             short level) :
        node_type{node_type},
        level{level} {
      this->right = nullptr;
      this->down = nullptr;
    };

    BaseNode(NodeType
             node_type,
             short level,
             KeyType
             node_key,
             ValueType item_value
    ) :
        BaseNode{node_type, level} {
      this->node_key = node_key;
      this->item_value = item_value;
    }

    /*
     * GetType() - Return the type of node
     *
     * This method does not allow overridding
     */
    inline NodeType GetType() const {
      return node_type;
    }

  };



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

  ///////////////////////////////////////////////////////////////////
  // Value Comparison Member
  ///////////////////////////////////////////////////////////////////

  /*
   * ValueCmpEqual() - Compares whether two values are equal
   */
  inline bool ValueCmpEqual(const ValueType &v1, const ValueType &v2) {
    return value_eq_obj(v1, v2);
  }

  class ForwardIterator;

  ForwardIterator Begin() {
    return ForwardIterator{this};
  }

  class ForwardIterator {
   public:
    ForwardIterator(SkipList *p_tree_p);
  };

  class EpochManager {
   public:
    SkipList *skiplist_p;

    // Garbage collection interval (milliseconds)
    constexpr static int GC_INTERVAL = 50;

    /*
     * struct GarbageNode - A linked list of garbages
     */
    struct GarbageNode {
      const BaseNode *node_p;

      // This does not have to be atomic, since we only
      // insert at the head of garbage list
      GarbageNode *next_p;
    };

    /*
     * struct EpochNode - A linked list of epoch node that records thread count
     *
     * This struct is also the head of garbage node linked list, which must
     * be made atomic since different worker threads will contend to insert
     * garbage into the head of the list
     */
    struct EpochNode {
      // We need this to be atomic in order to accurately
      // count the number of threads
      std::atomic<int> active_thread_count;

      // We need this to be atomic to be able to
      // add garbage nodes without any race condition
      // i.e. GC nodes are CASed onto this pointer
      std::atomic<GarbageNode *> garbage_list_p;

      // This does not need to be atomic since it is
      // only maintained by the epoch thread
      EpochNode *next_p;
    };

    // The head pointer does not need to be atomic
    // since it is only accessed by epoch manager
    EpochNode *head_epoch_p;

    // This does not need to be atomic because it is only written
    // by the epoch manager and read by worker threads. But it is
    // acceptable that allocations are delayed to the next epoch
    EpochNode *current_epoch_p;

    // This flag indicates whether the destructor is running
    // If it is true then GC thread should not clean
    // Therefore, strict ordering is required
    std::atomic<bool> exited_flag;

    // If GC is done with external thread then this should be set
    // to nullptr
    // Otherwise it points to a thread created by EpochManager internally
    std::thread *thread_p;

    // The counter that counts how many free is called
    // inside the epoch manager
    // NOTE: We cannot precisely count the size of memory freed
    // since sizeof(Node) does not reflect the true size, since
    // some nodes are embedded with complicated data structure that
    // maintains its own memory


    /*
     * Constructor - Initialize the epoch list to be a single node
     *
     * NOTE: We do not start thread here since the init of bw-tree itself
     * might take a long time
     */
    EpochManager(SkipList *skiplist_p) :
        skiplist_p{skiplist_p} {
      current_epoch_p = new EpochNode{};

      // These two are atomic variables but we could
      // simply assign to them
      current_epoch_p->active_thread_count = 0;
      current_epoch_p->garbage_list_p = nullptr;

      current_epoch_p->next_p = nullptr;

      head_epoch_p = current_epoch_p;

      // We allocate and run this later
      thread_p = nullptr;

      // This is used to notify the cleaner thread that it has ended
      exited_flag.store(false);

      // Initialize atomic counter to record how many
      // freed has been called inside epoch manager


      return;
    }

    /*
     * Destructor - Stop the worker thread and cleanup resources not freed
     *
     * This function waits for the worker thread using join() method. After the
     * worker thread has exited, it synchronously clears all epochs that have
     * not been recycled by calling ClearEpoch()
     *
     * NOTE: If no internal GC is started then thread_p would be a nullptr
     * and we neither wait nor free the pointer.
     */
    ~EpochManager() {
      // Set stop flag and let thread terminate
      // Also if there is an external GC thread then it should
      // check this flag everytime it does cleaning since otherwise
      // the un-thread-safe function ClearEpoch() would be ran
      // by more than 1 threads
      exited_flag.store(true);

      // If thread pointer is nullptr then we know the GC thread
      // is not started. In this case do not wait for the thread, and just
      // call destructor
      //
      // NOTE: The destructor routine is not thread-safe, so if an external
      // GC thread is being used then that thread should check for
      // exited_flag everytime it wants to do GC
      //
      // If the external thread calls ThreadFunc() then it is safe
      if (thread_p != nullptr) {
        skplist_printf("Waiting for thread\n");

        thread_p->join();

        // Free memory
        delete thread_p;

        skplist_printf("Thread stops\n");
      }

      // So that in the following function the comparison
      // would always fail, until we have cleaned all epoch nodes
      current_epoch_p = nullptr;

      // If all threads has exited then all thread counts are
      // 0, and therefore this should proceed way to the end
      ClearEpoch();

      // If we have a bug (currently there is one) then as a temporary
      // measure just force cleaning all epoches no matter whether they
      // are cleared or not
      if (head_epoch_p != nullptr) {
        skplist_printf("ERROR: After cleanup there is still epoch left\n");
        skplist_printf("==============================================\n");
        skplist_printf("DUMP\n");

        for (EpochNode *epoch_node_p = head_epoch_p;
             epoch_node_p != nullptr;
             epoch_node_p = epoch_node_p->next_p) {
          skplist_printf("Active thread count: %d\n",
                         epoch_node_p->active_thread_count.load());
          epoch_node_p->active_thread_count = 0;
        }

        skplist_printf("RETRY CLEANING...\n");
        ClearEpoch();
      }

      assert(head_epoch_p == nullptr);
      skplist_printf("Garbage Collector has finished freeing all garbage nodes\n");

      return;
    }

    /*
     * CreateNewEpoch() - Create a new epoch node
     *
     * This functions does not have to consider race conditions
     */
    void CreateNewEpoch() {
      skplist_printf("Creating new epoch...\n");

      EpochNode *epoch_node_p = new EpochNode{};

      epoch_node_p->active_thread_count = 0;
      epoch_node_p->garbage_list_p = nullptr;

      // We always append to the tail of the linked list
      // so this field for new node is always nullptr
      epoch_node_p->next_p = nullptr;

      // Update its previous node (current tail)
      current_epoch_p->next_p = epoch_node_p;

      // And then switch current epoch pointer
      current_epoch_p = epoch_node_p;



      return;
    }

    /*
     * AddGarbageNode() - This encapsulates BwTree::AddGarbageNode()
     */
    inline void AddGarbageNode(const BaseNode *node_p) {
      skiplist_p->AddGarbageNode(node_p);

      return;
    }

    inline EpochNode *JoinEpoch() {
      skiplist_p->UpdateLastActiveEpoch();

      return nullptr;
    }

    inline void LeaveEpoch(EpochNode *epoch_p) {
      skiplist_p->UpdateLastActiveEpoch();

      (void) epoch_p;
      return;
    }

    inline void PerformGarbageCollection() {
      skiplist_p->IncreaseEpoch();

      return;
    }

    /*
     * ClearEpoch() - Sweep the chain of epoch and free memory
     *
     * The minimum number of epoch we must maintain is 1 which means
     * when current epoch is the head epoch we should stop scanning
     *
     * NOTE: There is no race condition in this function since it is
     * only called by the cleaner thread
     */
    void ClearEpoch();

    /*
     * ThreadFunc() - The cleaner thread executes this every GC_INTERVAL ms
     *
     * This function exits when exit flag is set to true
     */
    void ThreadFunc() {
      // While the parent is still running
      // We do not worry about race condition here
      // since even if we missed one we could always
      // hit the correct value on next try
      while (exited_flag.load() == false) {
        //printf("Start new epoch cycle\n");
        PerformGarbageCollection();

        // Sleep for 50 ms
        std::chrono::milliseconds duration(GC_INTERVAL);
        std::this_thread::sleep_for(duration);
      }

      skplist_printf("exit flag is true; thread return\n");

      return;
    }

    /*
     * StartThread() - Start cleaner thread for garbage collection
     *
     * NOTE: This is not called in the constructor, and needs to be
     * called manually
     */
    void StartThread() {
      thread_p = new std::thread{[this]() { this->ThreadFunc(); }};

      return;
    }

  }; // Epoch manager

};

}  // End index namespace
}  // End peloton namespace
