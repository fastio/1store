// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
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

#include "store/util/coding.hh"
#include <cstring>

namespace store {

void encode_fixed32(char* buf, uint32_t value) {
    memcpy(buf, &value, sizeof(value));
}

void encode_fixed64(char* buf, uint64_t value) {
    memcpy(buf, &value, sizeof(value));
}

void put_fixed32(bytes& dst, uint32_t value) {
  char buf[sizeof(value)];
  encode_fixed32(buf, value);
  dst.append(buf, sizeof(buf));
}

void put_fixed64(bytes& dst, uint64_t value) {
  char buf[sizeof(value)];
  encode_fixed64(buf, value);
  dst.append(buf, sizeof(buf));
}

char* encode_varint32(char* dst, uint32_t v) {
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128;
  if (v < (1<<7)) {
    *(ptr++) = v;
  } else if (v < (1<<14)) {
    *(ptr++) = v | B;
    *(ptr++) = v>>7;
  } else if (v < (1<<21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = v>>14;
  } else if (v < (1<<28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = v>>21;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = (v>>21) | B;
    *(ptr++) = v>>28;
  }
  return reinterpret_cast<char*>(ptr);
}

void put_varint32(bytes& dst, uint32_t v) {
  char buf[5];
  char* ptr = encode_varint32(buf, v);
  dst.append(buf, ptr - buf);
}

char* encode_varint64(char* dst, uint64_t v) {
  static const int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B-1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

void put_varint64(bytes& dst, uint64_t v) {
  char buf[10];
  char* ptr = encode_varint64(buf, v);
  dst.append(buf, ptr - buf);
}

void put_length_prefixed_slice(bytes& dst, const bytes_view& value) {
  put_varint32(dst, value.size());
  dst.append(value.data(), value.size());
}

int varint_length(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

const char* get_varint32_ptr_fallback(const char* p,
                                   const char* limit,
                                   uint32_t& value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

bool get_varint32(bytes_view& input, uint32_t& value) {
  const char* p = input.data();
  const char* limit = p + input.size();
  const char* q = get_varint32_ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    input = bytes_view { q, static_cast<uint32_t>(limit - q) };
    return true;
  }
}

const char* get_varint64_ptr(const char* p, const char* limit, uint64_t& value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

bool get_varint64(bytes_view& input, uint64_t& value) {
  const char* p = input.data();
  const char* limit = p + input.size();
  const char* q = get_varint64_ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    input = bytes_view { q, static_cast<uint32_t>(limit - q) };
    return true;
  }
}

const char* get_length_prefixed_slice(const char* p, const char* limit, bytes_view& result) {
  uint32_t len = 0;
  p = get_varint32_ptr(p, limit, len);
  if (p == nullptr) return nullptr;
  if (p + len > limit) return nullptr;
  result = bytes_view {p, len };
  return p + len;
}

bool get_length_prefixed_slice(bytes_view& input, bytes_view& result) {
  uint32_t len = 0;
  if (get_varint32(input, len) &&
      input.size() >= len) {
    result = bytes_view { input.data(), len };
    input.remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

}  // namespace store 
