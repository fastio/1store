// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Modified by Peng Jian.
#pragma once

#include <stdint.h>
#include "store/log_format.hh"
#include "utils/bytes.hh"
#include "core/future.hh"
#include "core/file.hh"
#include "core/shared_ptr.hh"
#include "seastarx.hh"

namespace store {


namespace log {

template <class PartitionType>
bytes serialize_to_bytes(lw_shared_ptr<PartitionType> p) {
    assert(p);
    return p->serialize();
}

class writer {
 public:
  explicit writer(const bytes file_name);
  explicit writer(lw_shared_ptr<file> dest);
  writer(lw_shared_ptr<file>, uint64_t dest_length);

  ~writer() {}

  template<class PartitionType>
  future<> append(lw_shared_ptr<PartitionType> p) {
      return do_append(bytes_view { serialize_to_bytes (p) });
  }

 private:
  lw_shared_ptr<file> dest_;
  int block_offset_;       // Current offset in block
  size_t pos_ = 0;          // the current pos of file
  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];

  future<> do_append(bytes_view slice);
  future<> emit_physical_record(record_type type, const char* ptr, size_t length);

  // No copying allowed
  writer(const writer&) = delete;
  void operator=(const writer&) = delete;
};

}  // namespace log
}  // namespace store 
