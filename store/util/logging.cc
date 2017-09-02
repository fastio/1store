// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "store/util/logging.hh"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include "store/slice.hh"
#include "utils/bytes.hh"
#include <string>

namespace store {

void append_number_to(bytes* str, uint64_t num) {
    char buf[30];
    auto size = snprintf(buf, sizeof(buf), "%llu", (unsigned long long) num);
    str->append(buf, size);
}

void append_escaped_string_to(bytes* str, const slice& value) {
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

bytes escape_string(const slice& value) {
    bytes r;
    append_escaped_string_to(&r, value);
    return r;
}

bool consume_decimal_number(slice* in, uint64_t* val) {
    uint64_t v = 0;
    int digits = 0;
    while (!in->empty()) {
        char c = (*in)[0];
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
            in->remove_prefix(1);
        } else {
            break;
        }
    }
    *val = v;
    return (digits > 0);
}

}  // namespace store 
