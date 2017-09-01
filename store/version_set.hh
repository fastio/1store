// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#pragma once
#include <map>
#include <set>
#include <vector>
#include "store/dbformat.h"
#include "store/version_edit.h"

namespace store {

namespace log { class writer; }

class compaction;
class iterator;
class memtable;
class table_builder;
class table_cache;
class version;
class version_set;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
extern int find_file(const internal_key_comparator& icmp,
                    const std::vector<file_meta_data*>& files,
                    const slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==NULL represents a key smaller than all keys in the DB.
// largest==NULL represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
extern bool some_file_overlaps_range(
    const internal_key_comparator& icmp,
    bool disjoint_sorted_files,
    const std::vector<file_meta_data*>& files,
    const slice* smallest_user_key,
    const slice* largest_user_key);

class version {
 public:
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void add_iterators(const read_options&, std::vector<iterator*>* iters);

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  struct get_stats {
    file_meta_data* seek_file;
    int seek_file_level;
  };
  future<temporary_buffer<char>, get_stats, status> read(const read_options&, const lookup_key& key);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  bool update_stats(const get_stats& stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  bool record_read_sample(slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void ref();
  void unref();

  void get_overlapping_inputs(
      int level,
      const internal_key* begin,         // NULL means before all keys
      const internal_key* end,           // NULL means after all keys
      std::vector<file_meta_data*>* inputs);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==NULL represents a key smaller than all keys in the DB.
  // largest_user_key==NULL represents a key largest than all keys in the DB.
  bool overlap_in_level(int level,
                      const slice* smallest_user_key,
                      const slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  int pick_level_for_mem_table_output(const slice& smallest_user_key,
                                 const slice& largest_user_key);

  int num_files(int level) const { return files_[level].size(); }

  // Return a human readable string that describes this version's contents.
  bytes debug_string() const;

 private:
  friend class compaction;
  friend class version_set;

  class level_file_num_iterator;
  iterator* new_concatenating_iterator(const read_options&, int level) const;

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  void for_each_overlapping(slice user_key, slice internal_key,
                          void* arg,
                          bool (*func)(void*, int, file_meta_data*));

  version_set* vset_;            // VersionSet to which this Version belongs
  version* next_;               // Next version in linked list
  version* prev_;               // Previous version in linked list
  int refs_;                    // Number of live refs to this version

  // List of files per level
  std::vector<file_meta_data*> files_[config::kNumLevels];

  // Next file to compact based on seek stats.
  file_meta_data* file_to_compact_;
  int file_to_compact_level_;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  double compaction_score_;
  int compaction_level_;

  explicit version(version_set* vset)
      : vset_(vset), next_(this), prev_(this), refs_(0),
        file_to_compact_(NULL),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {
  }

  ~version();

  // No copying allowed
  version(const Version&) = delete;
  void operator=(const Version&) = delete;
};

class version_set {
 public:
  version_set(const std::string& dbname,
             const options* options,
             table_cache* table_cache,
             const internal_key_comparator*);
  ~version_set();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  status log_and_apply(version_edit* edit);

  // Recover the last saved descriptor from persistent storage.
  Ssatus recover(bool *save_manifest);

  // Return the current version.
  version* current() const { return current_; }

  // Return the current manifest file number
  uint64_t manifest_file_number() const { return manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t new_file_number() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  void reuse_file_number(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  int num_level_files(int level) const;

  // Return the combined file size of all files at the specified level.
  int64_t num_level_bytes(int level) const;

  // Return the last sequence number.
  uint64_t last_sequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  void set_last_sequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  void mark_file_number_used(uint64_t number);

  // Return the current log file number.
  uint64_t log_number() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t prev_log_number() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns NULL if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  lw_shared_ptr<compaction> pick_compaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns NULL if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  lw_shared_ptr<compaction> compact_range(
      int level,
      const internal_key* begin,
      const internal_key* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t max_next_level_overlapping_bytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  iterator* make_input_iterator(lw_shared_ptr<compaction> c);

  // Returns true iff some level needs a compaction.
  bool needs_compaction() const {
    version* v = current_;
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != NULL);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  void add_live_files(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t approximate_offset_of(version* v, const internal_key& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  //struct LevelSummaryStorage {
  //  char buffer[100];
  //};
  //const char* LevelSummary(LevelSummaryStorage* scratch) const;

 private:
  class builder;

  friend class compaction;
  friend class version;

  bool reuse_manifest(const std::string& dscname, const std::string& dscbase);

  void finalize(version* v);

  void get_range(const std::vector<file_meta_data*>& inputs,
                internal_key* smallest,
                internal_key* largest);

  void get_range2(const std::vector<file_meta_data*>& inputs1,
                 const std::vector<file_meta_data*>& inputs2,
                 internal_key* smallest,
                 internal_key* largest);

  void setup_other_inputs(lw_shared_ptr<compaction> c);

  // Save current contents to *log
  future<status> write_snapshot(lw_shared_ptr<log::writer> log);

  void append_version(lw_shared_ptr<version> v);

  const std::string dbname_;
  const options* const options_;
  lw_shared_ptr<table_cache> const table_cache_;
  const internal_key_comparator icmp_;
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  lw_shared_ptr<file> descriptor_file_;
  lw_shared_ptr<log::writer> descriptor_log_;
  version dummy_versions_;  // Head of circular doubly-linked list of versions.
  version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid internal_key.
  std::string compact_pointer_[config::kNumLevels];

  // No copying allowed
  version_set(const version_set&) = delete;
  void operator=(const version_set&) = delete;
};

// A Compaction encapsulates information about a compaction.
class compaction {
 public:
  ~compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  version_edit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  file_meta_data* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  uint64_t max_output_file_size() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool is_trivial_move() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void add_input_deletions(version_edit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool is_base_level_for_key(const slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool should_stop_before(const slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void release_inputs();

 private:
  friend class version;
  friend class version_set;

  compaction(const options* options, int level);

  int level_;
  uint64_t max_output_file_size_;
  version* input_version_;
  version_edit edit_;

  // Each compaction reads inputs from "level_" and "level_+1"
  std::vector<file_meta_data*> inputs_[2];      // The two sets of inputs

  // State used to check for number of of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  std::vector<file_meta_data*> grandparents_;
  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace store 
