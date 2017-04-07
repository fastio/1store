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

namespace redis {

namespace stdx = std::experimental;

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
    sstring _command;
    std::vector<sstring> _command_args;
    args_collection () : _command_args_count(0) {}
};

class item;
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
    sstring _key;
    size_t  _hash;
    redis_key(sstring&& key) : _key(std::move(key)), _hash(std::hash<sstring>()(_key)) {}
    redis_key(redis_key&& other) : _key(other._key), _hash(other._hash) {}
    redis_key(const sstring& key) : _key(key), _hash(std::hash<sstring>()(_key)) {}
    redis_key& operator = (redis_key&& o) {
        if (this != &o) {
            _key = std::move(o._key);
            _hash = o._hash;
            o._hash = 0;
        }
        return *this;
    }

    inline const size_t hash() const { return _hash; }
    inline const sstring& key() const { return _key; }
    inline const size_t size() const { return _key.size(); }
    inline const char* data() const { return _key.c_str(); }
};
/*
class redis_entry
{
private:
    sstring _key;
    size_t  _key_hash;
    union {
      uintptr_t  _ptr;
      uint64_t   _uint64;
      int64_t    _int64;
      double   _double;
    } _u { 0 };
};
*/
// The defination of `item was copied from apps/memcached
class list;
class dict;
class sorted_set;
class bitmap;
class item {
public:
    using time_point = expiration::time_point;
    using duration = expiration::duration;
private:
    friend class db;
    uint32_t _value_size;
    uint32_t _key_size;
    size_t   _key_hash;
    uint8_t  _type;
    uint8_t _volatile = false;
    expiration _expired;
    union {
      uintptr_t  _ptr;
      uint64_t _uint64;
      int64_t  _int64;
      double   _double;
    } _u { 0 };
    char* _appends = nullptr;
    friend class dict;
    static constexpr uint32_t field_alignment = alignof(void*);
public:
    template<typename... Args>
    static lw_shared_ptr<item> create(Args&&... args) {
        return make_lw_shared<item>(std::forward<Args>(args)...);
    }
    ~item();
    item(const redis_key& key, sstring&& value)
        : _value_size(value.size())
        , _key_size(key.size())
        , _key_hash(key.hash())
        , _type(REDIS_RAW_STRING)
    {
        size_t size = align_up(_value_size, field_alignment) + _key_size;
        _appends = new char[size];
        memcpy(_appends, value.c_str(), _value_size);
        if (_key_size > 0) {
            memcpy(_appends + align_up(_value_size, field_alignment), key.data(), _key_size);
        }
    }

    item(const redis_key& key, const std::experimental::string_view& value, sstring&& append)
        : _value_size(value.size() + append.size())
        , _key_size(key.size())
        , _key_hash(key.hash())
        , _type(REDIS_RAW_STRING)
    {
        size_t size = align_up(_value_size, field_alignment) + _key_size;
        _appends = new char[size];
        memcpy(_appends, value.data(), value.size());
        memcpy(_appends + value.size(), append.c_str(), append.size());
        if (_key_size > 0) {
            memcpy(_appends + align_up(_value_size, field_alignment), key.data(), _key_size);
        }
    }
    item(redis_key&& key)
        : _value_size(0)
        , _key_size(key.size())
        , _key_hash(key.hash())
        , _type(REDIS_RAW_ITEM)
    {
        size_t size = _key_size;
        _appends = new char[size];
        if (_key_size > 0) {
            memcpy(_appends, key.data(), _key_size);
        }
    }
    item(const redis_key& key)
        : _value_size(0)
        , _key_size(key.size())
        , _key_hash(key.hash())
        , _type(REDIS_RAW_ITEM)
    {
        size_t size = _key_size;
        _appends = new char[size];
        if (_key_size > 0) {
            memcpy(_appends, key.data(), _key_size);
        }
    }
    item(sstring&& value)
        : _value_size(value.size())
        , _key_size(0)
        , _key_hash(0)
        , _type(REDIS_RAW_ITEM)
    {
        size_t size = _value_size;
        _appends = new char[size];
        memcpy(_appends, value.c_str(), _value_size);
    }

    item(const redis_key& key, uint64_t value)
        : _value_size(0)
        , _key_size(key.size())
        , _key_hash(key.hash())
        , _type(REDIS_RAW_UINT64)
    {
        _u._uint64 = value;
        size_t size = _key_size;
        _appends = new char[size];
        if (_key_size > 0) {
            memcpy(_appends, key.data(), _key_size);
        }
    }

    item(const redis_key& key, double value)
        : _value_size(0)
        , _key_size(key.size())
        , _key_hash(key.hash())
        , _type(REDIS_RAW_DOUBLE)
    {
        _u._double = value;
        size_t size = _key_size;
        _appends = new char[size];
        if (_key_size > 0) {
            memcpy(_appends, key.data(), _key_size);
        }
    }

    item(const redis_key& key, int64_t value)
        : _value_size(0)
        , _key_size(key.size())
        , _key_hash(key.hash())
        , _type(REDIS_RAW_INT64)
    {
        _u._int64 = value;
        size_t size = _key_size;
        _appends = new char[size];
        if (_key_size > 0) {
            memcpy(_appends, key.data(), _key_size);
        }
    }

    item(const redis_key& key, list* ptr) : item(key, reinterpret_cast<uintptr_t>(ptr), REDIS_LIST) {}
    item(const redis_key& key, dict* ptr) : item(key, reinterpret_cast<uintptr_t>(ptr), REDIS_DICT) {}
    item(const redis_key& key, sorted_set* ptr) : item(key, reinterpret_cast<uintptr_t>(ptr), REDIS_ZSET) {}
    item(const redis_key& key, bitmap* ptr) : item(key, reinterpret_cast<uintptr_t>(ptr), REDIS_BITMAP) {}

    item(const redis_key& key, uintptr_t ptr, uint8_t type)
        : _value_size(0)
        , _key_size(key.size())
        , _key_hash(key.hash())
        , _type(type)
    {
        _u._ptr = ptr; //reinterpret_cast<uintptr_t>(ptr);
        size_t size = _key_size;
        _appends = new char[size];
        if (_key_size > 0) {
            memcpy(_appends, key.data(), _key_size);
        }
    }

public:
    inline const bool ever_expires() const {
        return _expired.ever_expires();
    }

    inline void set_never_expired() {
        return _expired.set_never_expired();
    }

    inline void set_expiry(const expiration& expiry) {
        _expired = expiry;
    }

    inline const clock_type::time_point get_timeout() const {
        return _expired.to_time_point();
    }

    inline bool cancel() const { return false; }

    const std::experimental::string_view key() const {
        return std::experimental::string_view(_appends + align_up(_value_size, field_alignment), _key_size);
    }

    const size_t key_hash() const { return _key_hash; }

    const std::experimental::string_view value() const {
        return std::experimental::string_view(_appends, _value_size);
    }

    item(const item&) = delete;
    item(item&&) = delete;


    inline list* list_ptr() const { return reinterpret_cast<list*>(_u._ptr); }
    inline dict* dict_ptr() const { return reinterpret_cast<dict*>(_u._ptr); }
    inline sorted_set* zset_ptr() const { return reinterpret_cast<sorted_set*>(_u._ptr); }
    inline bitmap* bitmap_ptr() const { return reinterpret_cast<bitmap*>(_u._ptr); }

    inline const uint64_t uint64() const { return _u._uint64; }
    inline const int64_t int64() const { return _u._int64; }
    inline const double Double() const { return _u._double; }

    inline const uint32_t value_size() const { return _value_size; }
    inline const uint32_t key_size() const { return _key_size; }

    inline const uint8_t type() const { return _type; }
};
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
