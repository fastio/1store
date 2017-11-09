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
#pragma once
#include "keys.hh"
#include "utils/managed_bytes.hh"
namespace redis {

static constexpr const int HLL_P = 14;
static constexpr const int HLL_BITS = 6;
static constexpr const int HLL_CARD_CACHE_SIZE = 8;
static constexpr const int HLL_BUCKET_COUNT = (1 << HLL_P);
static constexpr const int HLL_BYTES_SIZE = HLL_CARD_CACHE_SIZE + (HLL_BUCKET_COUNT * HLL_BITS + 7) / 8;

static constexpr const int HLL_BUCKET_COUNT_MAX = (1 << HLL_BITS) - 1;
class hll {
public:
    static size_t append(managed_bytes& data, const std::vector<bytes>& elements);
    static size_t count(managed_bytes& data);
    static size_t count(const uint8_t* merged_sources, size_t size);
    static size_t merge(managed_bytes& data, const uint8_t* merged_sources, size_t size); 
    static size_t merge(uint8_t* dest, size_t size, const bytes& merged_sources); 
};

}
