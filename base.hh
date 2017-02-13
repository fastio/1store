/*
 *  * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*  This file copy from Seastar's apps/memcached.
 *  Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 **/
#pragma once
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
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
};

struct args_collection {
    uint32_t _command_args_count;
    sstring _command;
    std::vector<sstring> _command_args;
    args_collection () : _command_args_count(0) {}
};

class item;
class redis_commands;
extern __thread redis_commands* _redis_commands_ptr;
inline redis_commands* redis_commands_ptr() {
    return _redis_commands_ptr;
}

using clock_type = lowres_clock;
static constexpr clock_type::time_point never_expire_timepoint = clock_type::time_point(clock_type::duration::min());

// The defination of `expiration was copied from apps/memcached
struct expiration {
    using time_point = clock_type::time_point;
    using duration   = time_point::duration;

    static constexpr uint32_t seconds_in_a_month = 60U * 60 * 24 * 30;
    time_point _time = never_expire_timepoint;

    expiration() {}

    expiration(clock_type::duration wc_to_clock_type_delta, uint32_t s) {
        using namespace std::chrono;

        static_assert(sizeof(clock_type::duration::rep) >= 8, "clock_type::duration::rep must be at least 8 bytes wide");

        if (s == 0U) {
            return; // means never expire.
        } else if (s <= seconds_in_a_month) {
            _time = clock_type::now() + seconds(s); // from delta
        } else {
            _time = time_point(seconds(s) + wc_to_clock_type_delta); // from real time
        }
    }

    bool ever_expires() {
        return _time != never_expire_timepoint;
    }

    time_point to_time_point() {
        return _time;
    }
};

class db;
struct redis_key {
    sstring _key;
    size_t  _hash;
    redis_key(sstring key) : _key(key), _hash(std::hash<sstring>()(_key)) {}
    redis_key(sstring key, size_t hash) : _key(key), _hash(hash) {}
    redis_key(redis_key&& other) : _key(other._key), _hash(other._hash) {}
    redis_key(const redis_key& o) : _key(o._key), _hash(o._hash) {}
    redis_key& operator=(const redis_key& o) {
        _key = o._key;
        _hash = o._hash;
        return *this;
    }
    redis_key& operator=(redis_key&& o) {
        _key = std::move(o._key);
        _hash = o._hash;
        o._hash = 0;
        return *this;
    }

    inline const size_t hash() const { return _hash; }
    inline const sstring& key() const { return _key; }
    inline const size_t size() const { return _key.size(); }
    inline const char* data() const { return _key.c_str(); }
};

// The defination of `item was copied from apps/memcached
class list;
class dict;
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
    expiration _expired;    
    bi::list_member_hook<> _timer_link;
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

    item(const redis_key& key, list* ptr, uint8_t type) : item(key, reinterpret_cast<uintptr_t>(ptr), type) {}
    item(const redis_key& key, dict* ptr, uint8_t type) : item(key, reinterpret_cast<uintptr_t>(ptr), type) {}

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
    clock_type::time_point get_timeout() {
        return _expired.to_time_point();
    }
    inline bool cancel() const { return false; }
    const std::experimental::string_view key() const {
        return std::experimental::string_view(_appends + align_up(_value_size, field_alignment), _key_size);
    }

    const size_t key_hash() { return _key_hash; }

    const std::experimental::string_view value() const {
        return std::experimental::string_view(_appends, _value_size);
    }

    item(const item&) = delete;
    item(item&&) = delete;


    inline list* list_ptr() { return reinterpret_cast<list*>(_u._ptr); }
    inline dict* dict_ptr() { return reinterpret_cast<dict*>(_u._ptr); }

    inline uint64_t uint64() { return _u._uint64; }
    inline int64_t int64() { return _u._int64; }
    inline int64_t Double() { return _u._double; }

    inline uint64_t incr(uint64_t step) {
        _u._uint64 += step;
        return _u._uint64;
    }
    inline int64_t incr(int64_t step) {
        _u._int64 += step;
        return _u._int64;
    }
    inline double incr(double step) {
        _u._double += step;
        return _u._double;
    }

    inline const uint32_t value_size() const { return _value_size; }
    inline const uint32_t key_size() const { return _key_size; }

    inline uint8_t type() const { return _type; }
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
static const sstring msg_type_string {"+string\r\n"};
static const sstring msg_type_none {"+none\r\n"};
static const sstring msg_type_list {"+list\r\n"};
static const sstring msg_type_set {"+set\r\n"};
static const sstring msg_type_zset {"+zset\r\n"};
static const sstring msg_type_hash {"+hash\r\n"};
} /* namespace redis */
