// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
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

#include "store/util/logging.hh"
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include "utils/bytes.hh"
#include <string>

namespace store {

void append_number_to(bytes* str, uint64_t num) {
    char buf[30];
    auto size = snprintf(buf, sizeof(buf), "%llu", (unsigned long long) num);
    str->append(buf, size);
}

void append_escaped_string_to(bytes* str, const bytes_view value) {
    std::string b; 
    for (size_t i = 0; i < value.size(); i++) {
        char c = value[i];
        if (c >= ' ' && c <= '~') {
            b.push_back(c);
        } else {
            char buf[10];
            snprintf(buf, sizeof(buf), "\\x%02x",
                    static_cast<unsigned int>(c) & 0xff);
            b.append(buf);
        }
    }
    str->append(b.c_str(), b.size());
}

bytes number_to_string(uint64_t num) {
    bytes r;
    append_number_to(&r, num);
    return r;
}

bytes escape_string(bytes_view value) {
    bytes r;
    append_escaped_string_to(&r, value);
    return r;
}

bool consume_decimal_number(bytes_view& in, uint64_t& val) {
    uint64_t v = 0;
    int digits = 0;
    while (!in.empty()) {
        char c = in[0];
        if (c >= '0' && c <= '9') {
            ++digits;
            // |delta| intentionally unit64_t to avoid Android crash (see log).
            const uint64_t delta = (c - '0');
            static const uint64_t kMaxUint64 = ~static_cast<uint64_t>(0);
            if (v > kMaxUint64/10 ||
                    (v == kMaxUint64/10 && delta > kMaxUint64%10)) {
                // Overflow
                return false;
            }
            v = (v * 10) + delta;
            in.remove_prefix(1);
        } else {
            break;
        }
    }
    val = v;
    return (digits > 0);
}

}  // namespace store 
