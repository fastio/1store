// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An iterator yields a sequence of key/value pairs from a source.
// The following class defines the interface.  Multiple implementations
// are provided by this library.  In particular, iterators are provided
// to access the contents of a Table or a DB.
//
// Multiple threads can invoke const methods on an iterator without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same iterator must use
// external synchronization.
#pragma once

#include "include/slice.h"
#include "include/status.h"

namespace store {

class iterator {
 public:
  iterator();
  virtual ~iterator();

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  virtual bool valid() const = 0;

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void seek_to_first() = 0;

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void seek_to_last() = 0;

  // Position at the first key in the source that is at or past target.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void seek(const slice& target) = 0;

  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void next() = 0;

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void prev() = 0;

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual slice key() const = 0;

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual slice value() const = 0;

  // If an error has occurred, return it.  Else return an ok status.
  virtual status status() const = 0;

  // Clients are allowed to register function/arg1/arg2 triples that
  // will be invoked when this iterator is destroyed.
  //
  // Note that unlike all of the preceding methods, this method is
  // not abstract and therefore clients should not override it.
  using cleanup_function_type = std::function<void(void*, void*)>; 
  void RegisterCleanup(cleanup_function_type&& function, void* arg1, void* arg2);

 private:
  struct cleanup {
    cleanup_function_type function;
    void* arg1;
    void* arg2;
    cleanup* next;
  };
  cleanup cleanup_;

  // No copying allowed
  iterator(const iterator&) = delete;
  void operator=(const iterator&) = delete;
};

// Return an empty iterator (yields nothing).
extern iterator* new_empty_iterator();

// Return an empty iterator with the specified status.
extern iterator* new_errori_terator(const status& status);

}
