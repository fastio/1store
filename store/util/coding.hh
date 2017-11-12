// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Endian-neutral encoding:
// * Fixed-length numbers are encoded with least-significant byte first
// * In addition we support variable length "varint" encoding
// * Strings are encoded prefixed by their length in varint format
//
//
//  Modified by Peng Jian.
//
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
#pragma once

#include <stdint.h>
#include <string.h>
#include <string>
#include "utils/bytes.hh"

// to uniform the code style.
//
namespace store {

// Standard Put... routines append to a string
extern void put_fixed32(bytes& dst, uint32_t value);
extern void put_fixed64(bytes& dst, uint64_t value);
extern void put_varint32(bytes& dst, uint32_t value);
extern void put_varint64(bytes& dst, uint64_t value);
extern void put_length_prefixed_slice(bytes& dst, const bytes& value);

// Standard Get... routines parse a value from the beginning of a slice
// and advance the slice past the parsed value.
extern bool get_varint32(bytes_view& input, uint32_t& value);
extern bool get_varint64(bytes_view& input, uint64_t& value);
extern bool get_length_prefixed_slice(bytes_view& input, bytes& result);

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// NULL on error.  These routines only look at bytes in the range
// [p..limit-1]
extern const char* get_varint32_ptr(const char* p,const char* limit, uint32_t& v);
extern const char* get_varint64_ptr(const char* p,const char* limit, uint64_t& v);

// Returns the length of the varint32 or varint64 encoding of "v"
extern int varint_length(uint64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// REQUIRES: dst has enough space for the value being written
extern void encode_fixed32(char* dst, uint32_t value);
extern void encode_fixed64(char* dst, uint64_t value);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
extern char* encode_varint32(char* dst, uint32_t value);
extern char* encode_varint64(char* dst, uint64_t value);

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

inline uint32_t encode_fixed32(const char* ptr) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
}

inline uint32_t decode_fixed32(const char* ptr) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
}


inline uint64_t decode_fixed64(const char* ptr) {
    // Load the raw bytes
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
}

// Internal routine for use by fallback path of GetVarint32Ptr
extern const char* get_varint32_ptr_fallback(const char* p,
                                          const char* limit,
                                          uint32_t& value);
inline const char* get_varint32_ptr(const char* p,
                                  const char* limit,
                                  uint32_t& value) {
  if (p < limit) {
    uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
    if ((result & 128) == 0) {
      value = result;
      return p + 1;
    }
  }
  return get_varint32_ptr_fallback(p, limit, value);
}

}
