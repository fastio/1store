// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <stdint.h>
#include "db/log_format.h"
#include "store/slice.h"
#include "store/status.h"

namespace store {


namespace log {

class writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit writer(lw_shared_ptr<file> dest);

  // Create a writer that will append data to "*dest".
  // "*dest" must have initial length "dest_length".
  // "*dest" must remain live while this Writer is in use.
  writer(lw_shared_ptr<file>, uint64_t dest_length);

  ~writer();

  future<status> add_record(const slice& slice);

 private:
  lw_shared_ptr<file> dest_;
  int block_offset_;       // Current offset in block
  size_t pos_ = 0;          // the current pos of file
  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];

  future<status> emit_physical_record(record_type type, const char* ptr, size_t length);

  // No copying allowed
  writer(const writer&) = delete;
  void operator=(const writer&) = delete;
};

}  // namespace log
}  // namespace store 
