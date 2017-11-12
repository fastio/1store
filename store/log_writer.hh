// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Modified by Peng Jian.
/*
* Pedis is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* You may obtain a copy of the License at
*
*     http://www.gnu.org/licenses
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
* 
*  Copyright (c) 2016-2026, Peng Jian, pengjian.uestc@gmail.com. All rights reserved.
*
*/
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
  explicit writer(file dest);

  ~writer() {}

  future<> append(bytes_view slice);

 private:
  file dest_;
  int block_offset_;       // Current offset in block
  size_t pos_ = 0;          // the current pos of file
  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[MAX_RECORD_TYPE + 1];

  future<> emit_physical_record(record_type type, const char* ptr, size_t length);

  // No copying allowed
  writer(const writer&) = delete;
  void operator=(const writer&) = delete;
};

}  // namespace log
}  // namespace store 
