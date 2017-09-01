// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <stdio.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

reader::Reporter::~Reporter() {
}

reader::reader(lw_shared_ptr<file> src, Reporter* reporter, bool checksum, uint64_t initial_offset)
    : file_reader_(file_reader)
    , reporter_(reporter)
    , checksum_(checksum)
    , backing_store_(new char[kBlockSize])
    , buffer_()
    , eof_(false)
    , last_record_offset_(0)
    , end_of_buffer_offset_(0)
    , initial_offset_(initial_offset)
    , resyncing_(initial_offset > 0)
{
}

reader::~reader() {
  delete[] backing_store_;
}

bool reader::skip_to_initial_block() {
  size_t offset_in_block = initial_offset_ % kBlockSize;
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // Don't search a block if we'd be in the trailer
  if (offset_in_block > kBlockSize - 6) {
    offset_in_block = 0;
    block_start_location += kBlockSize;
  }

  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    file_reader_->seek(block_start_location);
  }

  return true;
}

future<temporary_buffer<char>> reader::read_record() {
    if (last_record_offset_ < initial_offset_) {
        if (!skip_to_initial_block()) {
            return make_ready_future<temporary_buffer<char>>({});
        }
    }

    // Record offset of the logical record that we're reading
    // 0 is a dummy value to make compilers happy
    return do_with(bytes(), uint64_t { 0 }, bool { false }, [this] (auto& buffer, auto& prospective_record_offset, auto& in_fragmented_record) { 
        return repeat([this] () mutable {
            return read_physical_record().then([this, &buffer] (auto&& data, auto&& type) {
                // ReadPhysicalRecord may have only had an empty trailer remaining in its
                // internal buffer. Calculate the offset of the next physical record now
                // that it has returned, properly accounting for its header size.
                uint64_t physical_record_offset = end_of_buffer_offset_ - buffer_.size() - kHeaderSize - data.size();
                if (resyncing_) {
                    if (type == kMiddleType) {
                         return make_ready_future<stop_iteration>(stop_iteration::no);
                    } else if (type == kLastType) {
                         resyncing_ = false;
                         return make_ready_future<stop_iteration>(stop_iteration::no);
                    } else {
                         resyncing_ = false;
                    }
                }
            
                switch (type) {
                    case kFullType:
                        if (in_fragmented_record) {
                            if (buffer.empty())
                                in_fragmented_record = false;
                            else
                            {
                                //FIXME: throw exceptions.
                            }
                        }
                        prospective_record_offset = physical_record_offset;
                        last_record_offset_ = prospective_record_offset;
                        buffer.append(data);
                        return make_ready_future<stop_iteration>(stop_iteration::yes);

                    case kFirstType:
                        if (in_fragmented_record) {
                            if (buffer.empty())
                                in_fragmented_record = false;
                            else
                            {
                                //FIXME: throw exceptions.
                            }
                        }
                        prospective_record_offset = physical_record_offset;
                        buffer.append(data);
                        in_fragmented_record = true;
                        return make_ready_future<stop_iteration>(stop_iteration::no);

                    case kMiddleType:
                        if (!in_fragmented_record) {
                           //FIXME: throw exceptions.
                        } else {
                            buffer.append(data);
                        }
                        return make_ready_future<stop_iteration>(stop_iteration::no);

                    case kLastType:
                        if (!in_fragmented_record) {
                           //FIXME: throw exceptions.
                        } else {
                            buffer.append(data);
                            last_record_offset_ = prospective_record_offset;
                            return make_ready_future<stop_iteration>(stop_iteration::yes);
                        }
                    case kEof:
                        if (in_fragmented_record) {
                            // This can be caused by the writer dying immediately after
                            // writing a physical record but before completing the next; don't
                            // treat it as a corruption, just ignore the entire logical record.
                            buffer.clear(); 
                        }
                        return make_ready_future<stop_iteration>(stop_iteration::yes);

                    case kBadRecord:
                        if (in_fragmented_record) {
                            in_fragmented_record = false;
                            buffer.clear();
                        }
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    default:
                        in_fragmented_record = false;
                        buffer.clear();
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                }
            });
        }).finally([this, &buffer] () {
            return make_ready_future<temporary_buffer<char>>(buffer.release());
        });
    });
}

uint64_t reader::LastRecordOffset() {
  return last_record_offset_;
}

void reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, status::Corruption(reason));
}

void reader::ReportDrop(uint64_t bytes, const status& reason) {
  if (reporter_ != NULL &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

future<temporary_buffer<char>, record_type> reader::read_physical_record() {
    if (buffer_.size() < kHeaderSize) {
        if (!eof_) {
            // Last read was a full read, so this is a trailer to skip
            return file_reader_->read_exactly(kBlockSize).then([this] (temporary_buffer<char>& buffer) {
                buffer_ = slice { buffer.get(), buffer.size() }; 
                end_of_buffer_offset_ += buffer_.size();
                backing_store_ = std::move(buffer);
                if (buffer_.size() < kBlockSize) {
                    eof_ = true;
                }
                return read_physical_record();
            });
        } else {
            // Note that if buffer_ is non-empty, we have a truncated header at the
            // end of the file, which can be caused by the writer crashing in the
            // middle of writing the header. Instead of considering this an error,
            // just report EOF.
            buffer_.clear();
            return make_ready_future<temporary_buffer<char>, record_type>({ {}, kEof});
        }
    }

    // Parse the header
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);
    if (kHeaderSize + length > buffer_.size()) {
        size_t drop_size = buffer_.size();
        buffer_.clear();
        if (!eof_) {
          //ReportCorruption(drop_size, "bad record length");
            return make_ready_future<temporary_buffer<char>, record_type>({ {}, kBadRecord});
        }
        // If the end of the file has been reached without reading |length| bytes
        // of payload, assume the writer died in the middle of writing the record.
        // Don't report a corruption.
        return make_ready_future<temporary_buffer<char>, record_type>({ {}, kEof});
    }

    if (type == kZeroType && length == 0) {
        // Skip zero length record without reporting any drops since
        // such records are produced by the mmap based writing code in
        // env_posix.cc that preallocates file regions.
        buffer_.clear();
        return make_ready_future<temporary_buffer<char>, record_type>({ {}, kBadRecord});
    }

    // Check crc
    if (checksum_) {
        uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
        uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
        if (actual_crc != expected_crc) {
            // Drop the rest of the buffer since "length" itself may have
            // been corrupted and if we trust it, we could find some
            // fragment of a real log record that just happens to look
            // like a valid log record.
            size_t drop_size = buffer_.size();
            buffer_.clear();
            //ReportCorruption(drop_size, "checksum mismatch");
            return make_ready_future<temporary_buffer<char>, record_type>({ {}, kBadRecord});
        }
    }

    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length < initial_offset_) {
        result->clear();
        return make_ready_future<temporary_buffer<char>, record_type>({ {}, kBadRecord});
    }

    return make_ready_future<temporary_buffer<char>, record_type>({ {header + kHeaderSize, length}, type});
}

}  // namespace log
}  // namespace store 
