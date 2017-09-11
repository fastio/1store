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
#include "hll.hh"
#include <cmath>
namespace redis {

static constexpr const int HLL_P = 14;
static constexpr const int HLL_BITS = 6;
static constexpr const int HLL_CARD_CACHE_SIZE = 8;
static constexpr const int HLL_BUCKET_COUNT = (1 << HLL_P);
static constexpr const int HLL_BYTES_SIZE = HLL_CARD_CACHE_SIZE + (HLL_BUCKET_COUNT * HLL_BITS + 7) / 8;

static constexpr const int HLL_BUCKET_COUNT_MAX = (1 << HLL_BITS) - 1;
static constexpr const int HLL_BUCKET_COUNT_MASK = HLL_BUCKET_COUNT - 1; 
static constexpr const double ALPHA = 0.7213 / (1+1.079 / HLL_BUCKET_COUNT);
static constexpr const double ALPHA_BUCKET_COUNT_POWER_2 = ALPHA * HLL_BUCKET_COUNT * HLL_BUCKET_COUNT;
static constexpr const double PE[64] = { 
    1.0,   1.0 / (1ULL << 1), 1.0 / (1ULL << 2), 1.0 / (1ULL << 3), 1.0 / (1ULL << 4), 1.0 / (1ULL << 5), 1.0 / (1ULL << 6), 1.0 / (1ULL << 7), 1.0 / (1ULL << 8), 
    1.0 / (1ULL << 9), 1.0 / (1ULL << 10), 1.0 / (1ULL << 11), 1.0 / (1ULL << 12), 1.0 / (1ULL << 13), 1.0 / (1ULL << 14), 1.0 / (1ULL << 15), 1.0 / (1ULL << 16), 
    1.0 / (1ULL << 17), 1.0 / (1ULL << 18), 1.0 / (1ULL << 19), 1.0 / (1ULL << 20), 1.0 / (1ULL << 21), 1.0 / (1ULL << 22), 1.0 / (1ULL << 23), 1.0 / (1ULL << 24), 
    1.0 / (1ULL << 25), 1.0 / (1ULL << 26), 1.0 / (1ULL << 27), 1.0 / (1ULL << 28), 1.0 / (1ULL << 29), 1.0 / (1ULL << 30), 1.0 / (1ULL << 31), 1.0 / (1ULL << 32), 
    1.0 / (1ULL << 33), 1.0 / (1ULL << 34), 1.0 / (1ULL << 35), 1.0 / (1ULL << 36), 1.0 / (1ULL << 37), 1.0 / (1ULL << 38), 1.0 / (1ULL << 39), 1.0 / (1ULL << 40), 
    1.0 / (1ULL << 41), 1.0 / (1ULL << 42), 1.0 / (1ULL << 43), 1.0 / (1ULL << 44), 1.0 / (1ULL << 45), 1.0 / (1ULL << 46), 1.0 / (1ULL << 47), 1.0 / (1ULL << 48), 
    1.0 / (1ULL << 49), 1.0 / (1ULL << 50), 1.0 / (1ULL << 51), 1.0 / (1ULL << 52), 1.0 / (1ULL << 53), 1.0 / (1ULL << 54), 1.0 / (1ULL << 55), 1.0 / (1ULL << 56), 
    1.0 / (1ULL << 57), 1.0 / (1ULL << 58), 1.0 / (1ULL << 59), 1.0 / (1ULL << 60), 1.0 / (1ULL << 61), 1.0 / (1ULL << 62), 1.0 / (1ULL << 63) 
};

static inline void hll_invalidate_cache(uint8_t* cache, size_t size)
{
    if (size > 7) {
        cache[7] |= (1 << 7);
    }
}

static inline bool hll_is_valid_cache(const uint8_t* cache, size_t size)
{
    return size > 7 && (cache[7] & (1 << 7)) == 0;
}

static inline void hll_get_counter_on_bucket(uint8_t& counter, const uint8_t* p, long index)
{
    const uint8_t* _p = p;
    unsigned long _byte = index * HLL_BITS / 8;
    unsigned long _fb = index * HLL_BITS & 7;
    unsigned long _fb8 = 8 - _fb;
    unsigned long b0 = _p[_byte];
    unsigned long b1 = _p[_byte+1];
    counter = ((b0 >> _fb) | (b1 << _fb8)) & HLL_BUCKET_COUNT_MAX;
}

static inline void hll_set_counter_on_bucket(uint8_t* p, long index, uint8_t counter)
{
    uint8_t *_p = p;
    unsigned long _byte = index * HLL_BITS / 8;
    unsigned long _fb = index * HLL_BITS & 7;
    unsigned long _fb8 = 8 - _fb;
    unsigned long _v = counter;
    _p[_byte] &= ~(HLL_BUCKET_COUNT_MAX << _fb);
    _p[_byte] |= _v << _fb;
    _p[_byte+1] &= ~(HLL_BUCKET_COUNT_MAX >> _fb8);
    _p[_byte+1] |= _v >> _fb8;
}

bool hll_add(managed_bytes& data, const sstring& element)
{
    uint8_t oldcount = 0, count = 1;
    uint8_t* p = (uint8_t*)(data.data()) + HLL_CARD_CACHE_SIZE;
    auto hash = (uint64_t)(std::hash<sstring>()(element));
    //auto hash = murmur_hash_64a((uint8_t*)element.data(), element.size(), 0xadc83b19ULL);
    auto index = hash & HLL_BUCKET_COUNT_MASK;
    hash |= ((uint64_t) 1 << 63);
    uint64_t bit = HLL_BUCKET_COUNT;
    while ((hash & bit) == 0) {
        ++count;
        bit <<= 1;
    }
    hll_get_counter_on_bucket(oldcount, p, index);
    if (count > oldcount) {
        hll_set_counter_on_bucket(p, index, count);
        hll_invalidate_cache((uint8_t*)(data.data()), HLL_CARD_CACHE_SIZE);
        return true;
    }
    return false;
}

size_t hll::append(managed_bytes& data, const std::vector<sstring>& elements)
{
    size_t result = 0;
    for (size_t i = 0; i < elements.size(); ++i) {
        if (hll_add(data, elements[i])) {
            ++result;
        }
    }
    return result > 0;
}


static double hll_bucket_counter_sum(const uint8_t* data, size_t size, int& ez)
{
    ez = 0;
    double E = 0;

    const uint8_t *r = data;
    unsigned long r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15;
    for (int j = 0; j < 1024; j++) {
        r0 = r[0] & 63; if (r0 == 0) ez++;
        r1 = (r[0] >> 6 | r[1] << 2) & 63; if (r1 == 0) ez++;
        r2 = (r[1] >> 4 | r[2] << 4) & 63; if (r2 == 0) ez++;
        r3 = (r[2] >> 2) & 63; if (r3 == 0) ez++;
        r4 = r[3] & 63; if (r4 == 0) ez++;
        r5 = (r[3] >> 6 | r[4] << 2) & 63; if (r5 == 0) ez++;
        r6 = (r[4] >> 4 | r[5] << 4) & 63; if (r6 == 0) ez++;
        r7 = (r[5] >> 2) & 63; if (r7 == 0) ez++;
        r8 = r[6] & 63; if (r8 == 0) ez++;
        r9 = (r[6] >> 6 | r[7] << 2) & 63; if (r9 == 0) ez++;
        r10 = (r[7] >> 4 | r[8] << 4) & 63; if (r10 == 0) ez++;
        r11 = (r[8] >> 2) & 63; if (r11 == 0) ez++;
        r12 = r[9] & 63; if (r12 == 0) ez++;
        r13 = (r[9] >> 6 | r[10] << 2) & 63; if (r13 == 0) ez++;
        r14 = (r[10] >> 4 | r[11] << 4) & 63; if (r14 == 0) ez++;
        r15 = (r[11] >> 2) & 63; if (r15 == 0) ez++;

        E += (PE[r0] + PE[r1]) + (PE[r2] + PE[r3]) + (PE[r4] + PE[r5]) +
            (PE[r6] + PE[r7]) + (PE[r8] + PE[r9]) + (PE[r10] + PE[r11]) +
            (PE[r12] + PE[r13]) + (PE[r14] + PE[r15]);
        r += 12;
    }
    return E;
}

static size_t hll_read_card_from_cache(const managed_bytes& data)
{
    uint64_t card = 0;
    const uint8_t* p = (uint8_t*)(data.data());
    card = (uint64_t)p[0];
    card |= (uint64_t)p[1] << 8;
    card |= (uint64_t)p[2] << 16;
    card |= (uint64_t)p[3] << 24;
    card |= (uint64_t)p[4] << 32;
    card |= (uint64_t)p[5] << 40;
    card |= (uint64_t)p[6] << 48;
    card |= (uint64_t)p[7] << 56;
    return (size_t) card;
}

static void hll_write_card_to_cache(uint64_t card, managed_bytes& data)
{
    uint8_t* p = (uint8_t*)(data.data());
    p[0] =  card & 0xff;
    p[1] = (card >> 8) & 0xff;
    p[2] = (card >> 16) & 0xff;
    p[3] = (card >> 24) & 0xff;
    p[4] = (card >> 32) & 0xff;
    p[5] = (card >> 40) & 0xff;
    p[6] = (card >> 48) & 0xff;
    p[7] = (card >> 56) & 0xff;
}


static uint64_t compute_card(const uint8_t* data, size_t size)
{
    int ez = 0;
    auto S = hll_bucket_counter_sum(data, size, ez);

    S = (1/ S )* ALPHA_BUCKET_COUNT_POWER_2;

    if (S < HLL_BUCKET_COUNT * 2.5 && ez != 0) {
        S = HLL_BUCKET_COUNT * std::log ( double(HLL_BUCKET_COUNT) / double(ez));
    } else if (HLL_BUCKET_COUNT == 16384 && S < 72000) {
        double bias = 5.9119 * 1.0e-18 * (S * S * S * S)
                      -1.4253 * 1.0e-12 * (S * S * S)+
                      1.2940 * 1.0e-7 * ( S * S)
                      -5.2921 * 1.0e-3 * S +
                      83.3216;
        S -= S * (bias / 100);
    }
    return S;
}

size_t hll::count(managed_bytes& data)
{
    uint64_t card = 0;
    // read the card from cache.
    if (hll_is_valid_cache((uint8_t*)(data.data()), HLL_CARD_CACHE_SIZE)) {
        return hll_read_card_from_cache(data);
    }
    // compute the value of card.
    auto S = compute_card((uint8_t*)(data.data()) + HLL_CARD_CACHE_SIZE, data.size() - HLL_CARD_CACHE_SIZE);
    card = (uint64_t) S;
    hll_write_card_to_cache(card, data);
    return (size_t) card;
}

size_t hll::count(const uint8_t* merged_sources, size_t size)
{
    auto S = compute_card(merged_sources + HLL_CARD_CACHE_SIZE, size - HLL_CARD_CACHE_SIZE);
    return (size_t) S;
}

size_t hll::merge(managed_bytes& data, const uint8_t* merged_sources, size_t size)
{
    if (size != HLL_BYTES_SIZE) {
        return 0;
    }
    uint8_t* p = (uint8_t*)(data.data()) + HLL_CARD_CACHE_SIZE;
    const uint8_t* s = merged_sources + HLL_CARD_CACHE_SIZE;
    uint8_t counter = 0, counter_s = 0;
    for (size_t i = 0; i < HLL_BUCKET_COUNT; ++i) {
        hll_get_counter_on_bucket(counter_s, s, i);
        hll_get_counter_on_bucket(counter, p, i);
        if (counter_s > counter) {
            hll_set_counter_on_bucket(p, i, counter_s);
        }
    }
    hll_invalidate_cache((uint8_t*)data.data(), HLL_CARD_CACHE_SIZE);
    return 1;
}

size_t hll::merge(uint8_t* data, size_t size, const sstring& merged_sources)
{
    if (merged_sources.size() != HLL_BYTES_SIZE) {
        return 0;
    }
    uint8_t* p = data + HLL_CARD_CACHE_SIZE;
    const uint8_t* s = (uint8_t*)(merged_sources.data()) + HLL_CARD_CACHE_SIZE;
    uint8_t counter = 0, counter_s = 0;
    for (size_t i = 0; i < HLL_BUCKET_COUNT; ++i) {
        hll_get_counter_on_bucket(counter_s, s, i);
        hll_get_counter_on_bucket(counter, p, i);
        if (counter_s > counter) {
            hll_set_counter_on_bucket(p, i, counter_s);
        }
    }
    hll_invalidate_cache(data, HLL_CARD_CACHE_SIZE);
    return 1;
}
}
