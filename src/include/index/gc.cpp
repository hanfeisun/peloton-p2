//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bwtree.h
//
// Identification: src/backend/index/bwtree.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/logger.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <stack>
#include <set>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace peloton {
namespace index {

class EpochManager {
 private:
  // The type of the epoch
  typedef uint64_t epoch_t;
  static const uint64_t Interval = 50;

  static const uint32_t arraySize = 32;
  struct GarbageNodeArray {
    std::array<GarbageNode, arraySize> items;
    epoch_t epoch;
    uint32_t num_items = 0;
    GarbageNodeArray* next_group = nullptr;
  };

  struct GarbageNodeList {
    GarbageNodeArray* head = nullptr;
    GarbageNodeArray* tail = nullptr;
    GarbageNodeList* next = nullptr;

    ~GarbageNodeList() {
      Free(std::numeric_limits<epoch_t>::max());

      while (head != nullptr) {
        GarbageNodeArray* tmp = head;
        head = head->next_group;
        delete tmp;
      }
      if (next != nullptr) {
        delete next;
      }
    }

    GarbageNodeArray* GetFreeList(epoch_t epoch) {
      if (tail != nullptr && tail->epoch == epoch &&
          tail->num_items < arraySize - 1) {
        return tail;
      }


      GarbageNodeArray* group = new GarbageNodeArray();

      group->epoch = epoch;
      group->num_items = 0;
      group->next_group = nullptr;

      if (head == nullptr) {
        head = tail = group;
      } else {
        tail->next_group = group;
        tail = group;
      }
      return group;
    }

    void AddGarbageNode(GarbageNode garbage_node, epoch_t epoch) {
      GarbageNodeArray* to_add = GetFreeList(epoch);
      to_add->items[to_add->num_items++] = garbage_node;
    }

    void Free(epoch_t epoch) {
      uint32_t freed = 0;
      while (head != nullptr && head->epoch < epoch) {
        GarbageNodeArray* next = head->next_group;
        // All items in the head deletion group can be freed
        for (uint32_t i = 0; i < head->num_items; i++, freed++) {
          head->items[i].Free();
        }
        head = next;
      }
      if (freed > 0) {
        LOG_DEBUG("Freed %u objects before epoch %lu", freed, epoch);
      }
    }
  };

  struct ThreadBuffer {
    EpochManager* em;
    bool initialized = false;
    std::atomic<epoch_t> local_epoch{0};
    GarbageNodeList* deletion_list = new GarbageNodeList();
    uint32_t active_on_nodes = 0;

    ThreadBuffer(EpochManager* _em) : em(_em) { em->MakeItActive(this); }

    ~ThreadBuffer() { em->MakeItInactive(this); }
  };

  ThreadBuffer* GetThreadBuffer(EpochManager* em) {
    static thread_local ThreadBuffer thread_state{em};
    return &thread_state;
  }

  // Constructor
  EpochManager()
      : global_epoch(0),
        min_epoch(0),
        active_num(0),
        isStop(false),
        gc_thread(std::thread{&EpochManager::GlobalCounter, this}) {}

  // Destructor
  ~EpochManager() {
    isStop.store(true);
    gc_thread.join();
  }

  void MakeItActive(ThreadBuffer* tb) {
    tb->initialized = true;
    if (active_threads.find(std::this_thread::get_id()) ==
        active_threads.end()) {
      active_threads[std::this_thread::get_id()] = tb;
      ++active_num;
    }
  }

  void MakeItInactive(ThreadBuffer* tb) {

    // Remove state from map
    active_threads.erase(std::this_thread::get_id());

    tb->deletion_list->next = ReadyToDelete;
    ReadyToDelete = tb->deletion_list;
    tb->deletion_list = nullptr;

    --active_num;
  }

 public:
  static EpochManager& GetInstance() {
    static EpochManager instance_;
    return instance_;
  }

  void GlobalCounter() {
    std::chrono::milliseconds sleep_duration{Interval};
    while (!isStop.load()) {
      std::this_thread::sleep_for(sleep_duration);
      // Increment global epoch
      global_epoch++;
      {
        // Find the min epoch number among active threads. 

        epoch_t min = global_epoch.load();
        for (const auto& iter : active_threads) {
          ThreadBuffer* state = iter.second;
          epoch_t local_epoch = state->local_epoch.load();
          if (local_epoch < min) {
            min = local_epoch;
          }
        }
        min_epoch.store(min);

        // Try to cleanup zombied deletion lists
        GarbageNodeList* curr = ReadyToDelete;
        while (curr != nullptr) {
          curr->Free(min);
          curr = curr->next;
        }
      }
      LOG_DEBUG("Global epoch: %lu, min epoch: %lu, # states: %u",
                global_epoch.load(), min_epoch.load(), num_states);
    }

    LOG_DEBUG("Shutting down epoch ticker thread");
    while (active_num.load() > 0) {
      std::this_thread::sleep_for(sleep_duration);
    }
    LOG_DEBUG(
        "All threads unactive_num, deleting all memory from deletion lists");
    {

      if (ReadyToDelete != nullptr) {
        delete ReadyToDelete;
      }
    }
    LOG_DEBUG("Epoch management thread done");
  }

  void EnterEpoch() {
    auto* self_state = GetThreadBuffer(this);
    if (self_state->active_on_nodes++ == 0) {
      self_state->local_epoch.store(GetCurrentEpoch());
    }
  }

  void AddGarbageNode(GarbageNode garbage_node) {
    auto* self_state = GetThreadBuffer(this);

    auto* deletion_list = self_state->deletion_list;
    deletion_list->AddGarbageNode(garbage_node, self_state->local_epoch.load());
  }

  void ExitEpoch() {
    auto* self_state = GetThreadBuffer(this);
    if (--self_state->active_on_nodes == 0) {
      auto* deletion_list = self_state->deletion_list;
      deletion_list->next = ReadyToDelete;
      ReadyToDelete = deletion_list;
      deletion_list = nullptr;
    }
  }

  epoch_t GetCurrentEpoch() const { 
    return global_epoch.load(); 
  }

 private:
  // The global epoch number
  std::atomic<epoch_t> global_epoch;
    
  // The current minimum epoch that any transaction is in
  std::atomic<epoch_t> min_epoch;

  std::atomic<uint32_t> active_num;

  std::atomic<bool> isStop;
  std::thread gc_thread;

  std::unordered_map<std::thread::id, ThreadBuffer*> active_threads;
  GarbageNodeList* ReadyToDelete;
};

}  // End index namespace
}  // End peloton namespace