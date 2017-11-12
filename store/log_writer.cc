// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

//
// Modified by Peng Jian.
//
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
#include "store/log_writer.hh"
#include <stdint.h>
#include "store/util/coding.hh"
#include "store/util/crc32c.hh"
#include "store/priority_manager.hh"
namespace store {
namespace log {

static void init_type_crc(uint32_t* type_crc) {
  for (int i = 0; i <= MAX_RECORD_TYPE; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::value(&t, 1);
  }
}

writer::writer(lw_shared_ptr<file> dest)
    : dest_(dest)
    , block_offset_(0)
    , pos_ (0)
{
  init_type_crc(type_crc_);
}

future<> writer::do_append(const bytes_view data) {
    // Fragment the record if necessary and emit it.  Note that if slice
    // is empty, we still want to iterate once to emit a single
    // zero-length record
    return do_with(bytes_view { data }, size_t { data.size() }, bool { true }, [this] (auto& data, auto& left, auto& begin) {
        return repeat([this, &data, &left, &begin] () mutable {
            const char* ptr = data.data();
            const int leftover = BLOCK_SIZE - block_offset_;
            assert(leftover >= 0);
            if (leftover < HEADER_SIZE) {
                // Switch to a new block
                if (leftover > 0) {
                  // Fill the trailer (literal below relies on HEADER_SIZE being 7)
                  assert(HEADER_SIZE == 7);
                  const bytes zeros("\x00\x00\x00\x00\x00\x00");
                  auto&& priority_class = get_local_commitlog_priority();
                  return dest_->dma_write(pos_, zeros.data(), leftover, priority_class).then([this] (auto s) {
                      pos_ += s;
                      block_offset_ = 0;
                      return make_ready_future<stop_iteration>(stop_iteration::no);
                  });
                }
             }

             // Invariant: we never leave < HEADER_SIZE bytes in a block.
             assert(BLOCK_SIZE - block_offset_ - HEADER_SIZE >= 0);
             const size_t avail = BLOCK_SIZE - block_offset_ - HEADER_SIZE;
             const size_t fragment_length = (left < avail) ? left : avail;

             record_type type;
             auto end = (left == fragment_length);
             if (begin && end) {
                 type = record_type::full;
             } else if (begin) {
                 type = record_type::first;
             } else if (end) {
                 type = record_type::last;
             } else {
                 type = record_type::middle;
             }
             return this->emit_physical_record(type, ptr, fragment_length).then([this, &data, &left, &begin, fragment_length] {
                 data = bytes_view {data.data() + fragment_length, data.size() - fragment_length};
                 begin = false;
                 if (left > 0) {
                     return make_ready_future<stop_iteration>(stop_iteration::no);
                 }
                 // all data was appended to the file.
                 return make_ready_future<stop_iteration>(stop_iteration::yes);
             });
        }).finally ([this] {
             return dest_->flush().then([] {
                 return make_ready_future<>();
             });
        });
    });
}

future<> writer::emit_physical_record(record_type t, const char* ptr, size_t n) {
    assert(n <= 0xffff);  // Must fit in two bytes
    assert(block_offset_ + HEADER_SIZE + n <= BLOCK_SIZE);

    // Format the header
    char buf[HEADER_SIZE];
    buf[4] = static_cast<char>(n & 0xff);
    buf[5] = static_cast<char>(n >> 8);
    buf[6] = static_cast<char>(t);

    // Compute the crc of the record type and the payload.
    uint32_t crc = crc32c::extend(type_crc_[t], ptr, n);
    crc = crc32c::mask(crc);                 // Adjust for storage
    encode_fixed32(buf, crc);

    // Write the header and the payload
    auto&& priority_class = get_local_commitlog_priority();
    return dest_->dma_write(pos_, buf, HEADER_SIZE, priority_class).then([this, ptr, n] (auto s) {
        pos_ += HEADER_SIZE;
        block_offset_ += HEADER_SIZE;
        auto&& priority_class = get_local_commitlog_priority();
        return dest_->dma_write(pos_, ptr, n, priority_class).then([this, n] (auto s) {
            pos_ += n;
            block_offset_ += n;
            return make_ready_future<>();
        });
    });
}

}  // namespace log
}  // namespace store 
