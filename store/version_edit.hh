// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"

namespace store {

class version_set;

struct file_meta_data {
  int refs;
  int allowed_seeks;          // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;         // File size in bytes
  internal_key smallest;       // Smallest internal key served by table
  internal_key largest;        // Largest internal key served by table

  file_meta_data() : refs(0), allowed_seeks(1 << 30), file_size(0) { }
};

class version_edit {
 public:
  version_edit() { clear(); }
  ~version_edit() { }

  void clear();

  void set_comparator_name(const slice& name) {
    has_comparator_ = true;
    comparator_ = name.to_string();
  }
  void set_log_number(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void set_prev_log_number(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void set_next_file(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void set_last_sequence(sequence_number seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const internal_key& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see version_set::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void add_file(int level, uint64_t file,
               uint64_t file_size,
               const internal_key& smallest,
               const internal_key& largest) {
    file_meta_data f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  void delete_file(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void encode_to(std::string* dst) const;
  status decode_from(const slice& src);

  std::string debug_string() const;

 private:
  friend class version_set;

  typedef std::set< std::pair<int, uint64_t> > deleted_file_set;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  sequence_number last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector< std::pair<int, internal_key> > compact_pointers_;
  DeletedFileSet deleted_files_;
  std::vector< std::pair<int, file_meta_data> > new_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
