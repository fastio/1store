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
#include "db.hh"
#include <random>
#include <chrono>
#include <algorithm>
#include "util/log.hh"
#include "structures/bits_operation.hh"
#include "core/metrics.hh"
#include "structures/hll.hh"
#include "partition.hh"

using logger =  seastar::logger;
static logger db_log ("db");

namespace redis {


class rand_generater final
{
    friend class database;
public:
    inline static size_t rand_less_than(const size_t size) {
        static thread_local rand_generater _rand;
        return _rand.rand() % size;
    }
private:
    rand_generater()
        : _re(std::chrono::system_clock::now().time_since_epoch().count())
        , _dist(0, std::numeric_limits<size_t>::max())
    {
    }

    ~rand_generater()
    {
    }

    inline size_t rand()
    {
        return _dist(_re);
    }

    std::default_random_engine _re;
    std::uniform_int_distribution<size_t> _dist;
};

distributed<database> _the_database;

database::database()
{
    using namespace std::chrono;
    setup_metrics();
}

database::~database()
{
    with_allocator(allocator(), [this] {
        db_log.info("total {} entries were released in cache", _cache.size());
        _cache.flush_all();
    });
}

size_t database::sum_expiring_entries()
{
    return _cache.expiring_size();
}

void database::setup_metrics()
{
    namespace sm = seastar::metrics;
}

future<scattered_message_ptr> database::set(const redis_key& rk, bytes& val, long expired, uint32_t flag)
{
    return with_allocator(allocator(), [this, &rk, &val, expired, flag] {
        auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), val);
        bool result = true;
        if (_cache.insert_if(entry, expired, flag & FLAG_SET_NX, flag & FLAG_SET_XX)) {
        }
        else {
            result = false;
            current_allocator().destroy<cache_entry>(entry);
        }
        if (result && _enable_write_disk) {
            auto partition_entry = make_sstring_partition(rk.key(), val);
            return _store->write(_write_opt, to_decorated_key(rk), std::move(partition_entry)).then([this] {
                return reply_builder::build(msg_ok);
            });
        }
        return reply_builder::build(result ? msg_ok : msg_nil);
    });
}

future<scattered_message_ptr> database::del(const redis_key& rk)
{
    return _cache.with_entry_run(rk, [this, &rk] (cache_entry* e) {
        if (!e) return reply_builder::build(msg_zero);
        auto result =  _cache.erase(*e);
        if (result && _enable_write_disk) {
            auto partition_entry = make_removable_partition(rk.key());
            return _store->write(_write_opt, to_decorated_key(rk), std::move(partition_entry)).then([this] {
                return reply_builder::build(msg_one);
            });
        }
        return reply_builder::build(result ? msg_one : msg_zero);
    });
}

future<scattered_message_ptr> database::get(const redis_key& rk)
{
    // all keys should be cached in the memory.
    return _cache.with_entry_run(rk, [this] (const cache_entry* e) {
       if (e && e->type_of_bytes() == false) {
           return reply_builder::build(msg_type_err);
       }
       else {
           return reply_builder::build<false, true>(e);
       }
    });
}
future<> database::start()
{
    return make_ready_future<>();
}

future<> database::stop()
{
    return make_ready_future<>();
}
}
