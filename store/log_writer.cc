// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

//
// Modified by Peng Jian.
//
#include "store/log_writer.hh"

#include <stdint.h>
#include "store/util/coding.hh"
#include "store/util/crc32c.hh"

namespace store {
namespace log {

static void init_type_crc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

writer::writer(const bytes& file_name)
{
    // mock
}

writer::writer(lw_shared_ptr<file> dest)
    : dest_(dest)
    , block_offset_(0)
    , pos_ (0)
{
  init_type_crc(type_crc_);
}

writer::writer(lw_shared_ptr<file> dest, uint64_t dest_length)
    : dest_(dest)
    , block_offset_(dest_length % kBlockSize),
    , pos_(0)
{
  init_type_crc(type_crc_);
}

writer::~writer() {
}

future<> writer::append(const bytes_view data) {
    // Fragment the record if necessary and emit it.  Note that if slice
    // is empty, we still want to iterate once to emit a single
    // zero-length record
    return do_with(bytes_view { data }, size_t { data.size() }, bool { true }, [this] (auto& data, auto& left, auto& begin) {
        return repeat([this, &data, &left, &begin] () mutable {
            const char* ptr = data.data();
            const int leftover = kBlockSize - block_offset_;
            assert(leftover >= 0);
            if (leftover < kHeaderSize) {
                // Switch to a new block
                if (leftover > 0) {
                  // Fill the trailer (literal below relies on kHeaderSize being 7)
                  assert(kHeaderSize == 7);
                  const bytes zeros("\x00\x00\x00\x00\x00\x00");
                  return dest_->dma_write(pos_, leftover)).then([this] (auto s) {
                      pos_ += s;
                      block_offset_ = 0;
                      return make_ready_future<stop_iteration>(stop_iteration::no);
                  });
                }
             }

             // Invariant: we never leave < kHeaderSize bytes in a block.
             assert(kBlockSize - block_offset_ - kHeaderSize >= 0);
             const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
             const size_t fragment_length = (left < avail) ? left : avail;

             record_type type;
             auto end = (left == fragment_length);
             auto begin = kp.begin; 
             if (begin && end) {
                 type = kFullType;
             } else if (begin) {
                 type = kFirstType;
             } else if (end) {
                 type = kLastType;
             } else {
                 type = kMiddleType;
             }
             return emit_physical_record(type, ptr, fragment_length).then([this, &data, &left, &begin] {
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
    assert(block_offset_ + kHeaderSize + n <= kBlockSize);

    // Format the header
    char buf[kHeaderSize];
    buf[4] = static_cast<char>(n & 0xff);
    buf[5] = static_cast<char>(n >> 8);
    buf[6] = static_cast<char>(t);

    // Compute the crc of the record type and the payload.
    uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
    crc = crc32c::Mask(crc);                 // Adjust for storage
    EncodeFixed32(buf, crc);

    // Write the header and the payload
    return dest_->dma_write(slice(buf, kHeaderSize)).then([this, ptr, n] (auto s) {
        pos_ += kHeaderSize;
        block_offset_ += kHeaderSize;
        return dest_->dma_write(slice(ptr, n)).then([this, n] (auto s) {
            pos_ += n;
            block_offset_ += n;
            return make_ready_future<>();
        });
    });
}

}  // namespace log
}  // namespace store 
