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

/*  This file copy from Seastar's apps/memcached.
 *  Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */
#pragma once
#include <iomanip>
#include <sstream>
#include <functional>
#include <vector>
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/timer-set.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/units.hh"
#include "core/distributed.hh"
#include "core/vector-data-sink.hh"
#include "core/bitops.hh"
#include "core/slab.hh"
#include "core/align.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include <unistd.h>
#include  <experimental/vector>
namespace stdx = std::experimental;

namespace redis {

enum {
    FLAG_SET_EX = 1 << 0,
    FLAG_SET_PX = 1 << 1,
    FLAG_SET_NX = 1 << 2,
    FLAG_SET_XX = 1 << 3,
};

enum {
    REDIS_RAW_UINT64,
    REDIS_RAW_INT64,
    REDIS_RAW_DOUBLE,
    REDIS_RAW_STRING,
    REDIS_RAW_OBJECT, // for data struct
    REDIS_RAW_ITEM,   // for data item
    REDIS_LIST,
    REDIS_DICT,
    REDIS_SET,
    REDIS_ZSET,
    REDIS_BITMAP,
};

struct args_collection {
    uint32_t _command_args_count;
    std::vector<sstring> _command_args;
    std::vector<sstring> _tmp_keys;
    std::unordered_map<sstring, sstring> _tmp_key_values;
    std::unordered_map<sstring, double> _tmp_key_scores;
    std::vector<std::pair<sstring, sstring>> _tmp_key_value_pairs;
    args_collection () : _command_args_count(0) {}
};

using clock_type = lowres_clock;
static constexpr clock_type::time_point never_expire_timepoint = clock_type::time_point(clock_type::duration::min());

// The defination of `expiration was copied from apps/memcached
struct expiration {
    using time_point = clock_type::time_point;
    using duration   = time_point::duration;
    time_point _time = never_expire_timepoint;

    expiration() {}

    expiration(long s) {
        using namespace std::chrono;
        static_assert(sizeof(clock_type::duration::rep) >= 8, "clock_type::duration::rep must be at least 8 bytes wide");

        if (s == 0U) {
            return; // means never expire.
        } else {
            _time = clock_type::now() + milliseconds(s);
        }
    }

    inline const bool ever_expires() const {
        return _time != never_expire_timepoint;
    }

    inline const time_point to_time_point() const {
        return _time;
    }

    inline void set_never_expired() {
        _time = never_expire_timepoint;
    }
};

class db;
struct redis_key {
    sstring& _key;
    size_t  _hash;
    redis_key(sstring& key) : _key(key), _hash(std::hash<sstring>()(_key)) {}
    redis_key& operator = (const redis_key& o) {
        if (this != &o) {
            _key = o._key;
            _hash = o._hash;
        }
        return *this;
    }
    inline unsigned get_cpu() const { return _hash % smp::count; }
    inline const size_t hash() const { return _hash; }
    inline const sstring& key() const { return _key; }
    inline const size_t size() const { return _key.size(); }
    inline const char* data() const { return _key.c_str(); }
};
// The defination of `item was copied from apps/memcached
static const sstring msg_crlf {"\r\n"};
static const sstring msg_ok {"+OK\r\n"};
static const sstring msg_pong {"+PONG\r\n"};
static const sstring msg_err = {"-ERR\r\n"};
static const sstring msg_zero = {":0\r\n"};
static const sstring msg_one = {":1\r\n"};
static const sstring msg_neg_one = {":-1\r\n"};
static const sstring msg_null_blik = {"$-1\r\n"};
static const sstring msg_null_multi_bulk = {"*-1\r\n"};
static const sstring msg_empty_multi_bulk = {"*0\r\n"};
static const sstring msg_type_err = {"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"};
static const sstring msg_nokey_err = {"-ERR no such key\r\n"};
static const sstring msg_syntax_err = {"-ERR syntax error\r\n"};
static const sstring msg_same_object_err = {"-ERR source and destination objects are the same\r\n"};
static const sstring msg_out_of_range_err = {"-ERR index out of range\r\n"};
static const sstring msg_not_integer_err = {"-ERR ERR hash value is not an integer\r\n" };
static const sstring msg_not_float_err = {"-ERR ERR hash value is not an float\r\n" };
static const sstring msg_str_tag = {"+"};
static const sstring msg_num_tag = {":"};
static const sstring msg_sigle_tag = {"*"};
static const sstring msg_batch_tag = {"$"};
static const sstring msg_not_found = {"+(nil)\r\n"};
static const sstring msg_nil = {"+(nil)\r\n"};
static constexpr const int REDIS_OK = 0;
static constexpr const int REDIS_ERR = 1;
static constexpr const int REDIS_NONE = -1;
static constexpr const int REDIS_WRONG_TYPE = -2;
static const sstring msg_type_string {"+string\r\n"};
static const sstring msg_type_none {"+none\r\n"};
static const sstring msg_type_list {"+list\r\n"};
static const sstring msg_type_set {"+set\r\n"};
static const sstring msg_type_zset {"+zset\r\n"};
static const sstring msg_type_hash {"+hash\r\n"};

static constexpr const int HLL_P = 14;
static constexpr const int HLL_BITS = 6;
static constexpr const int HLL_CARD_CACHE_SIZE = 8;
static constexpr const int HLL_BUCKET_COUNT = (1 << HLL_P);
static constexpr const int HLL_BYTES_SIZE = HLL_CARD_CACHE_SIZE + (HLL_BUCKET_COUNT * HLL_BITS + 7) / 8;

static constexpr const int GEO_HASH_STEP_MAX  = 26; /* 26*2 = 52 bits. */
/* Limits from EPSG:900913 / EPSG:3785 / OSGEO:41001 */
static constexpr const double GEO_LAT_MIN = -85.05112878;
static constexpr const double GEO_LAT_MAX = 85.05112878;
static constexpr const double GEO_LAT_SCALE = GEO_LAT_MAX - GEO_LAT_MIN;
static constexpr const double GEO_LAT_MIN_STD = -90;
static constexpr const double GEO_LAT_MAX_STD = 90;
static constexpr const double GEO_LAT_STD_SCALE = GEO_LAT_MAX_STD - GEO_LAT_MIN_STD;
static constexpr const double GEO_LONG_MIN = -180;
static constexpr const double GEO_LONG_MAX = 180;
static constexpr const double GEO_LONG_SCALE = GEO_LONG_MAX - GEO_LONG_MIN;
// @brief Earth's quatratic mean radius for WGS-84
static constexpr const double EARTH_RADIUS_IN_METERS = 6372797.560856;
static constexpr const double MERCATOR_MAX = 20037726.37;
static constexpr const double MERCATOR_MIN = -20037726.37;
//
//const d
// some flags
static const int ZADD_NONE = 0;
static constexpr const int ZADD_INCR = (1 << 0); // increment the score instead of setting it.
static constexpr const int ZADD_NX   = (1 << 1); // don't touch elements not already existing.
static constexpr const int ZADD_XX   = (1 << 2); // only touch elements already exisitng.
static constexpr const int ZADD_CH   = (1 << 3); // number elementes were changed.

static constexpr const int ZAGGREGATE_MIN = (1 << 0);
static constexpr const int ZAGGREGATE_MAX = (1 << 1);
static constexpr const int ZAGGREGATE_SUM = (1 << 2);

static constexpr const int GEODIST_UNIT_M  = (1 << 0);
static constexpr const int GEODIST_UNIT_KM = (1 << 1);
static constexpr const int GEODIST_UNIT_MI = (1 << 2);
static constexpr const int GEODIST_UNIT_FT = (1 << 3);

static constexpr const int GEORADIUS_ASC         = (1 << 0);
static constexpr const int GEORADIUS_DESC        = (1 << 1);
static constexpr const int GEORADIUS_WITHCOORD   = (1 << 2);
static constexpr const int GEORADIUS_WITHSCORE   = (1 << 3);
static constexpr const int GEORADIUS_WITHHASH    = (1 << 4);
static constexpr const int GEORADIUS_WITHDIST    = (1 << 5);
static constexpr const int GEORADIUS_COUNT       = (1 << 6);
static constexpr const int GEORADIUS_STORE_SCORE = (1 << 7);
static constexpr const int GEORADIUS_STORE_DIST  = (1 << 8);
static constexpr const int GEO_UNIT_M      = (1 << 9);
static constexpr const int GEO_UNIT_KM     = (1 << 10);
static constexpr const int GEO_UNIT_MI     = (1 << 11);
static constexpr const int GEO_UNIT_FT     = (1 << 12);

static constexpr const size_t BITMAP_MAX_OFFSET  = (1 << 31);
} /* namespace redis */
