// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <stdint.h>
#include "store/log_format.h"
#include "utils/bytes.hh"
#include "core/future.hh"
#include "core/file.hh"
#include "core/shared_ptr.hh"
#include "seastarx.hh"

namespace store {


namespace log {

class writer {
 public:
  explicit writer(lw_shared_ptr<file> dest);
  writer(lw_shared_ptr<file>, uint64_t dest_length);

  ~writer();

  future<> append(const bytes_view slice);

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
