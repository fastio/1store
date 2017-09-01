// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// table_builder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a table_builder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same table_builder must use
// external synchronization.

#pragma once
#include <stdint.h>
#include "include/options.h"
#include "include/status.h"

namespace store {

class block_builder;
class block_handle;

class table_builder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  table_builder(const options& options, file& file);

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~table_builder();

  // Change the options used by this builder.  Note: only some of the
  // option fields can be changed after construction.  If a field is
  // not allowed to change dynamically and its value in the structure
  // passed to the constructor is different from its value in the
  // structure passed to this method, this method will return an error
  // without changing any fields.
  status change_options(const options& options);

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  future<status> add(const slice& key, const slice& value);

  // Advanced operation: flush any buffered key/value pairs to file.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  void flush();

  // Return non-ok iff some error has been detected.
  status status() const;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  future<status> finish();

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void abandon();

  // Number of calls to Add() so far.
  uint64_t num_entries() const;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t file_size() const;

 private:
  bool ok() const { return status().ok(); }
  void write_block(block_builder* block, block_handle* handle);
  void write_raw_block(const slice& data, compression_type, block_handle* handle);

  struct rep;
  rep* rep_;

  // No copying allowed
  table_builder(const table_builder&) = delete;
  void operator=(const table_builder&) = delete;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_
