// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <vector>

#include <stdint.h>
#include "slice.h"

namespace redis {

struct options;

class block_builder {
 public:
  explicit block_builder(const options* options);

  // Reset the contents as if the BlockBuilder was just constructed.
  void reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void add(const slice& key, const slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  slice finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t current_size_estimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const {
    return buffer_.empty();
  }

  block_builder(const block_builder&) = delete;
  void operator=(const block_builder&) = delete;

 private:
  const options*        options_;
  bytes                 buffer_;      // Destination buffer
  std::vector<uint32_t> restarts_;    // Restart points
  int                   counter_;     // Number of entries emitted since restart
  bool                  finished_;    // Has Finish() been called?
  bytes                 last_key_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
