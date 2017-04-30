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
*  Copyright (c) 2016-2026, Peng Jian, pstack@163.com. All rights reserved.
*
*/
#include "bits_operation.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/sharded.hh"
#include "common.hh"
namespace redis {
// 512M bytes
static const size_t MAX_BYTE_COUNT = 1024 * 1024 * 512 - 1;
static const size_t RESIZE_STEP    = 16;

bool bits_operation::set(managed_bytes& o, size_t offset, bool value)
{
    auto index = offset >> 3;
    if (index > o.size()) {
        size_t append_size = RESIZE_STEP;
        while (append_size < index) append_size+= RESIZE_STEP;
        o.extend(append_size, 0);
    }
    uint8_t byte_val = uint8_t(o[index]);
    auto bit = 7 - (offset & 0x7);
    auto bit_val = byte_val & (1 << bit);

    byte_val &= ~(1 << bit);
    byte_val |= ((value ? 0x1 : 0x0) << bit);
    o[index] = byte_val;
    return bit_val > 0;
}

bool bits_operation::get(const managed_bytes& o, size_t offset)
{
    auto offset_in_bytes = offset >> 3;
    if (offset_in_bytes > BITMAP_MAX_OFFSET || offset_in_bytes >= o.size()) {
        return false;
    }
    uint8_t byte_val = uint8_t(o[offset_in_bytes]);
    auto bit = 7 - (offset & 0x7);
    auto bit_val = byte_val & (1 << bit);
    return bit_val > 0;
}

size_t bits_operation::count(const managed_bytes& o, long start, long end)
{
    if (start < 0) start = static_cast<size_t>(start) + o.size();
    if (end < 0) end = static_cast<size_t>(end) + o.size();
    if (start < 0) start = 0;
    if (end < 0) end = 0;
    if (end >= static_cast<long>(o.size())) end = static_cast<long>(o.size()) - 1;
    size_t bits_count = 0, count = static_cast<size_t>(end - start) + 1;
    size_t p = start, p4 = 0;
    static const unsigned char bits_in_byte[256] = {
        0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,
        1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,
        1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,
        2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,
        1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,
        2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,
        2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,
        3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,4,5,5,6,5,6,6,7,5,6,6,7,6,7,7,8
    };

    while((unsigned long)(o[p]) & 3 && count) {
        uint8_t v = uint8_t(o[p++]);
        bits_count += static_cast<size_t>(bits_in_byte[static_cast<unsigned int>(v)]);
        count--;
    }

    p4 = p;
    uint32_t d1 = 0, d2 = 0, d3 = 0, d4 = 0, d5 = 0, d6 = 0, d7 = 0;
    while(count>=28) {
        d1 = uint8_t(o[p4++]);
        d2 = uint8_t(o[p4++]);
        d3 = uint8_t(o[p4++]);
        d4 = uint8_t(o[p4++]);
        d5 = uint8_t(o[p4++]);
        d6 = uint8_t(o[p4++]);
        d7 = uint8_t(o[p4++]);
        count -= 28;

        d1 = d1 - ((d1 >> 1) & 0x55555555);
        d1 = (d1 & 0x33333333) + ((d1 >> 2) & 0x33333333);
        d2 = d2 - ((d2 >> 1) & 0x55555555);
        d2 = (d2 & 0x33333333) + ((d2 >> 2) & 0x33333333);
        d3 = d3 - ((d3 >> 1) & 0x55555555);
        d3 = (d3 & 0x33333333) + ((d3 >> 2) & 0x33333333);
        d4 = d4 - ((d4 >> 1) & 0x55555555);
        d4 = (d4 & 0x33333333) + ((d4 >> 2) & 0x33333333);
        d5 = d5 - ((d5 >> 1) & 0x55555555);
        d5 = (d5 & 0x33333333) + ((d5 >> 2) & 0x33333333);
        d6 = d6 - ((d6 >> 1) & 0x55555555);
        d6 = (d6 & 0x33333333) + ((d6 >> 2) & 0x33333333);
        d7 = d7 - ((d7 >> 1) & 0x55555555);
        d7 = (d7 & 0x33333333) + ((d7 >> 2) & 0x33333333);
        bits_count += ((((d1 + (d1 >> 4)) & 0x0F0F0F0F) +
                    ((d2 + (d2 >> 4)) & 0x0F0F0F0F) +
                    ((d3 + (d3 >> 4)) & 0x0F0F0F0F) +
                    ((d4 + (d4 >> 4)) & 0x0F0F0F0F) +
                    ((d5 + (d5 >> 4)) & 0x0F0F0F0F) +
                    ((d6 + (d6 >> 4)) & 0x0F0F0F0F) +
                    ((d7 + (d7 >> 4)) & 0x0F0F0F0F))* 0x01010101) >> 24;
    }
    p = p4;
    while(count--) {
        uint8_t v = uint8_t(o[p++]);
        bits_count += static_cast<size_t>(bits_in_byte[static_cast<unsigned int>(v)]);
    }
    return bits_count;
}
}
