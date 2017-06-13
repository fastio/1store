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
#include "bits_operation.hh"
#include "core/metrics.hh"
#include "hll.hh"

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

database::database()
{
    using namespace std::chrono;
    setup_metrics();
}

database::~database()
{
    with_allocator(allocator(), [this] {
        for (size_t i = 0; i < DEFAULT_DB_COUNT; ++i) {
            db_log.info("before release cache [{}] entries {}", i, _cache_stores[i].size());
            _cache_stores[i].flush_all();
            db_log.info("after release cache [{}] entries {}", i, _cache_stores[i].size());
        }
    });
}

void database::setup_metrics()
{
    namespace sm = seastar::metrics;
    _metrics.add_group("db", {
        sm::make_gauge("get", [this] { return _stats._get; }, sm::description("GET")),
        sm::make_gauge("get_hit", [this] { return _stats._get_hit; }, sm::description("GET HIT")),
        sm::make_gauge("set", [this] { return _stats._set; }, sm::description("SET")),
        sm::make_gauge("del", [this] { return _stats._del; }, sm::description("DEL")),
        sm::make_gauge("total_entries", [this] { return _stats._total_entries; }, sm::description("Total entries")),
        sm::make_gauge("total_string_entries", [this] { return _stats._total_string_entries; }, sm::description("Total string entries")),
        sm::make_gauge("total_dict_entries", [this] { return _stats._total_dict_entries; }, sm::description("Total dict entries")),
        sm::make_gauge("total_set_entries", [this] { return _stats._total_set_entries; }, sm::description("Total set entries")),
        sm::make_gauge("total_sorted_set_entries", [this] { return _stats._total_sorted_set_entries; }, sm::description("Total sorted set entries")),
        sm::make_gauge("total_geo_entries", [this] { return _stats._total_geo_entries; }, sm::description("Total geo entries")),
     });
}

bool database::set_direct(const redis_key& rk, sstring& val, long expire, uint32_t flag)
{
    return with_allocator(allocator(), [this, &rk, &val] {
        auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), val);
        if (current_store().replace(entry)) {
            ++_stats._total_entries;
        }
        ++_stats._set;
        return true;
    });
}

future<scattered_message_ptr> database::set(const redis_key& rk, sstring& val, long expire, uint32_t flag)
{
    return with_allocator(allocator(), [this, &rk, &val] {
        auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), val);
        current_store().replace(entry);
        ++_stats._set;
        ++_stats._total_entries;
        return reply_builder::build(msg_ok);
    });
}

bool database::del_direct(const redis_key& rk)
{
    return with_allocator(allocator(), [this, &rk] {
        ++_stats._del;
        auto result =  current_store().erase(rk);
        if (result)
            --_stats._total_entries;
        return result;
    });
}

future<scattered_message_ptr> database::del(const redis_key& rk)
{
    return with_allocator(allocator(), [this, &rk] {
        auto result =  current_store().erase(rk);
        ++_stats._del;
        if (result)
            --_stats._total_entries;
        return reply_builder::build(result ? msg_one : msg_zero);
    });
}

bool database::exists_direct(const redis_key& rk)
{
    return current_store().exists(rk);
}

future<scattered_message_ptr> database::exists(const redis_key& rk)
{
    auto result = current_store().exists(rk);
    return reply_builder::build(result ? msg_one : msg_zero);
}

future<scattered_message_ptr> database::counter_by(const redis_key& rk, int64_t step, bool incr)
{
    return with_allocator(allocator(), [this, &rk, step, incr] {
        return current_store().with_entry_run(rk, [this, &rk, step, incr] (cache_entry* e) {
            if (!e) {
                // not exists
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), int64_t{step});
                current_store().replace(entry);
                return reply_builder::build<false, true>(entry);
            }
            if (!e->type_of_integer()) {
                return reply_builder::build(msg_type_err);
            }
            e->value_integer_incr(incr ? step : -step);
            return reply_builder::build<false, true>(e);
        });
    });
}

future<scattered_message_ptr> database::append(const redis_key& rk, sstring& val)
{
    return with_allocator(allocator(), [this, &rk, &val] {
        return current_store().with_entry_run(rk, [this, &rk, &val] (cache_entry* e) {
            if (!e) {
                // not exists
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), val);
                current_store().replace(entry);
                return reply_builder::build(val.size());
            }
            if (!e->type_of_bytes()) {
                return reply_builder::build(msg_type_err);
            }
            return with_linearized_managed_bytes([this, e, &val] {
                size_t new_size = e->value_bytes_size() + val.size();
                auto data = std::unique_ptr<bytes_view::value_type[]>(new bytes_view::value_type[new_size]);
                std::copy_n(e->value_bytes_data(), e->value_bytes_size(), data.get());
                std::copy_n(val.data(), val.size(), data.get() + e->value_bytes_size());
                auto new_value = current_allocator().construct<managed_bytes>(data.get(), new_size);
                auto& old_value = e->value_bytes();
                current_allocator().destroy<managed_bytes>(&old_value);
                e->value_bytes() = std::move(*new_value);
                return reply_builder::build(new_size);
            });
        });
    });
}

future<scattered_message_ptr> database::get(const redis_key& rk)
{
    return with_linearized_managed_bytes([this, &rk] {
        return current_store().with_entry_run(rk, [this] (const cache_entry* e) {
           ++_stats._get;
           if (e && e->type_of_bytes() == false) {
               return reply_builder::build(msg_type_err);
           }
           else {
               if (e != nullptr) ++_stats._get_hit;
               return reply_builder::build<false, true>(e);
           }
        });
    });
}

future<scattered_message_ptr> database::strlen(const redis_key& rk)
{
    return with_linearized_managed_bytes([this, &rk] {
        return current_store().with_entry_run(rk, [] (const cache_entry* e) {
            if (e) {
                if (e->type_of_bytes()) {
                    return reply_builder::build(e->value_bytes_size());
                }
                return reply_builder::build(msg_type_err);
            }
            return reply_builder::build(msg_zero);
        });
   });
}

future<scattered_message_ptr> database::type(const redis_key& rk)
{
    return reply_builder::build(msg_nil);
}

future<scattered_message_ptr> database::expire(const redis_key& rk, long expired)
{
    auto result = current_store().expire(rk, expired);
    return reply_builder::build(result ? msg_one : msg_zero);
}

future<scattered_message_ptr> database::persist(const redis_key& rk)
{
    return reply_builder::build(msg_nil);
}

future<scattered_message_ptr> database::push(const redis_key& rk, sstring& val, bool force, bool left)
{
    return with_allocator(allocator(), [this, &rk, &val, force, left] () {
        return current_store().with_entry_run(rk, [this, &rk, &val, force, left] (cache_entry* o) {
            auto e = o;
            if (!e) {
                 if (!force) {
                     return reply_builder::build(msg_err);
                 }
                // create new list object
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::list_initializer());
                current_store().insert(entry);
                e = entry;
            }
            if (e->type_of_list() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& list = e->value_list();
            left ? list.insert_head(val) : list.insert_tail(val);
            return reply_builder::build(list.size());
        });
    });
}

future<scattered_message_ptr> database::push_multi(const redis_key& rk, std::vector<sstring>& values, bool force, bool left)
{
    return with_allocator(allocator(), [this, &rk, &values, force, left] () {
        return current_store().with_entry_run(rk, [this, &rk, &values, force, left] (cache_entry* o) {
            auto e = o;
            if (!e) {
                 if (!force) {
                     return reply_builder::build(msg_err);
                 }
                // create new list object
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::list_initializer());
                current_store().insert(entry);
                e = entry;
            }
            if (e->type_of_list() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& list = e->value_list();
            for (auto& val : values) {
                left ? list.insert_head(val) : list.insert_tail(val);
            }
            return reply_builder::build(list.size());
        });
    });
}

future<scattered_message_ptr> database::pop(const redis_key& rk, bool left)
{
    return with_allocator(allocator(), [this, &rk, left] () {
        return current_store().with_entry_run(rk, [this, &rk, left] (cache_entry* e) {
            if (!e) {
                return reply_builder::build(msg_nil);
            }
            if (e->type_of_list() == false) {
                return reply_builder::build(msg_type_err);
            }
            return with_linearized_managed_bytes([this, &rk, e, left] () {
                auto& list = e->value_list();
                assert(!list.empty());
                auto reply = reply_builder::build(left ? list.front() : list.back());
                left ? list.pop_front() : list.pop_back();
                if (list.empty()) {
                   current_store().erase(rk);
                }
                return reply;
            });
        });
    });
}

future<scattered_message_ptr> database::llen(const redis_key& rk)
{
    return current_store().with_entry_run(rk, [&rk] (const cache_entry* e) {
        if (!e) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_list() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& list = e->value_list();
        return reply_builder::build(list.size());
    });
}

future<scattered_message_ptr> database::lindex(const redis_key& rk, long idx)
{
    return current_store().with_entry_run(rk, [&rk, idx] (const cache_entry* e) {
        if (!e) {
            return reply_builder::build(msg_nil);
        }
        if (e->type_of_list() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& list = e->value_list();
        auto index = alignment_index_base_on(list.size(), idx);
        if (list.index_out_of_range(index)) {
            return reply_builder::build(msg_nil);
        }
        auto& result = list.at(static_cast<size_t>(index));
        return reply_builder::build(result);
    });
}

future<scattered_message_ptr> database::lrange(const redis_key& rk, long start, long end)
{
    return current_store().with_entry_run(rk, [&rk, &start, &end] (const cache_entry* e) {
        if (!e) {
            return reply_builder::build(msg_err);
        }
        if (e->type_of_list() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& list = e->value_list();
        start = database::alignment_index_base_on(list.size(), start);
        end = database::alignment_index_base_on(list.size(), end);
        if (start < 0) start = 0;
        if (end >= static_cast<long>(list.size())) end = static_cast<size_t>(list.size()) - 1;
        std::vector<const managed_bytes*> data;
        if (start < end) {
           for (auto i = start; i < end; ++i) {
              const auto& b = list.at(static_cast<size_t>(i));
              data.push_back(&b);
           }
        }
        return reply_builder::build(data);
    });
}

future<scattered_message_ptr> database::lrem(const redis_key& rk, long count, sstring& val)
{
    return with_allocator(allocator(), [this, &rk, count, &val] {
        return current_store().with_entry_run(rk, [this, &rk, &val, &count] (cache_entry* e) {
            if (!e) {
                return reply_builder::build(msg_err);
            }
            if (e->type_of_list() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& list = e->value_list();
            size_t removed = 0;
            if (count == 0) removed = list.trem<true, true>(val, count);
            else if (count > 0) removed = list.trem<false, true>(val, count);
            else removed = list.trem<false, false>(val, count);
            if (list.empty()) {
                current_store().erase(rk);
            }
            return reply_builder::build(removed);
        });
    });
}

future<scattered_message_ptr> database::linsert(const redis_key& rk, sstring& pivot, sstring& val, bool after)
{
    return with_allocator(allocator(), [this, &rk, &pivot, &val, after] {
        return current_store().with_entry_run(rk, [this, &rk, &val, &pivot, after] (cache_entry* e) {
            if (!e) {
                return reply_builder::build(msg_zero);
            }
            if (e->type_of_list() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& list = e->value_list();
            auto index = list.index_of(pivot);
            if (list.index_out_of_range(index)) {
                return reply_builder::build(msg_zero);
            }
            if (after) {
                if (index == list.size()) list.insert_tail(val);
                else  list.insert_at(++index, val);
            }
            else {
               if (index == 0) list.insert_head(val);
               else list.insert_at(--index, val);
            }
            return reply_builder::build(msg_one);
        });
    });
}

future<scattered_message_ptr> database::lset(const redis_key& rk, long idx, sstring& val)
{
    return with_allocator(allocator(), [this, &rk, idx, &val] {
        return current_store().with_entry_run(rk, [this, &rk, idx, &val] (cache_entry* e) {
            if (!e) {
                return reply_builder::build(msg_nokey_err);
            }
            if (e->type_of_list() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& list = e->value_list();
            auto nidx = database::alignment_index_base_on(list.size(), idx);
            if (list.index_out_of_range(nidx)) {
                return reply_builder::build(msg_out_of_range_err);
            }
            auto& old = list.at(static_cast<size_t>(nidx));
            current_allocator().destroy<managed_bytes>(&old);
            auto entry = current_allocator().construct<managed_bytes>(bytes_view{reinterpret_cast<const signed char*>(val.data()), val.size()});
            list.at(idx) = std::move(*entry);
            return reply_builder::build(msg_ok);
        });
    });
}

future<scattered_message_ptr> database::ltrim(const redis_key& rk, long start, long end)
{
    return with_allocator(allocator(), [this, &rk, start, end] {
        return current_store().with_entry_run(rk, [this, &rk, start, end] (cache_entry* e) {
            if (!e) {
                return reply_builder::build(msg_ok);
            }
            if (e->type_of_list() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& list = e->value_list();
            auto nstart = database::alignment_index_base_on(list.size(), start);
            if (nstart < 0) nstart = 0;
            auto nend = database::alignment_index_base_on(list.size(), end);
            if (nstart > nend || nstart > static_cast<long>(list.size()) || nend < 0) {
                list.clear();
            }
            list.trim(static_cast<size_t>(nstart), static_cast<size_t>(nend));
            if (list.empty()) {
                current_store().erase(rk);
            }
            return reply_builder::build(msg_ok);
        });
    });
}

future<scattered_message_ptr> database::hset(const redis_key& rk, sstring& key, sstring& val)
{
    return with_allocator(allocator(), [this, &rk, &key, &val] {
        return current_store().with_entry_run(rk, [this, &rk, &key, &val] (cache_entry* o) {
            auto e = o;
            if (!e) {
                // the rk was not exists, then create it.
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::dict_initializer());
                current_store().insert(entry);
                e = entry;
            }
            if (e->type_of_map() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& map = e->value_map();
            bool exists = map.exists(key);
            auto entry = current_allocator().construct<dict_entry>(key, val);
            map.insert(entry);
            return reply_builder::build(exists ? msg_zero : msg_one);
        });
    });
}

future<scattered_message_ptr> database::hincrby(const redis_key& rk, sstring& key, int64_t delta)
{
    return with_allocator(allocator(), [this, &rk, &key, delta] {
        return current_store().with_entry_run(rk, [this, &rk, &key, delta] (cache_entry* o) {
            auto e = o;
            if (!e) {
                // the rk was not exists, then create it.
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::dict_initializer());
                current_store().insert(entry);
                e = entry;
            }
            if (e->type_of_map() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& map = e->value_map();
            return map.with_entry_run(key, [&map, &key, delta] (dict_entry* d) {
                if (!d) {
                    auto entry = current_allocator().construct<dict_entry>(key, delta);
                    map.insert(entry);
                    return reply_builder::build<false, true>(entry);
                }
                if (!d->type_of_integer()) {
                    return reply_builder::build(msg_not_integer_err);
                }
                d->value_integer_incr(delta);
                return reply_builder::build<false, true>(d);
            });
        });
    });
}

future<scattered_message_ptr> database::hincrbyfloat(const redis_key& rk, sstring& key, double delta)
{
    return with_allocator(allocator(), [this, &rk, &key, delta] {
        return current_store().with_entry_run(rk, [this, &rk, &key, delta] (cache_entry* o) {
            auto e = o;
            if (!e) {
                // the rk was not exists, then create it.
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::dict_initializer());
                current_store().insert(entry);
                e = entry;
            }
            if (e->type_of_map() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& map = e->value_map();
            return map.with_entry_run(key, [&map, &key, delta] (dict_entry* d) {
                if (!d) {
                    auto entry = current_allocator().construct<dict_entry>(key, delta);
                    map.insert(entry);
                    return reply_builder::build<false, true>(entry);
                }
                if (!d->type_of_float()) {
                    return reply_builder::build(msg_not_float_err);
                }
                d->value_float_incr(delta);
                return reply_builder::build<false, true>(d);
            });
        });
    });
}

future<scattered_message_ptr> database::hmset(const redis_key& rk, std::unordered_map<sstring, sstring>& kvs)
{
    return with_allocator(allocator(), [this, &rk, &kvs] {
        return current_store().with_entry_run(rk, [this, &rk, &kvs] (cache_entry* o) {
            auto e = o;
            if (!e) {
                // the rk was not exists, then create it.
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::dict_initializer());
                current_store().insert(entry);
                e = entry;
            }
            if (e->type_of_map() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& map = e->value_map();
            bool result = false;
            for (auto& kv : kvs) {
               auto entry = current_allocator().construct<dict_entry>(kv.first, kv.second);
               result = map.insert(entry);
               if (!result) break;
            }
            return reply_builder::build(result ? msg_ok : msg_err);
        });
    });
}

future<scattered_message_ptr> database::hget(const redis_key& rk, sstring& key)
{
    return current_store().with_entry_run(rk, [this, &key] (cache_entry* e) {
        if (!e) {
            return reply_builder::build(msg_err);
        }
        if (e->type_of_map() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& map = e->value_map();
        return map.with_entry_run(key, [] (const dict_entry* d) {
            return with_linearized_managed_bytes([d] {
                return reply_builder::build<false, true>(d);
            });
        });
    });
}

future<scattered_message_ptr> database::hdel_multi(const redis_key& rk, std::vector<sstring>& keys)
{
    return with_allocator(allocator(), [this, &rk, &keys] {
        return current_store().with_entry_run(rk, [this, &rk, &keys] (cache_entry* e) {
            if (!e) {
                return reply_builder::build(msg_zero);
            }
            if (e->type_of_map() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& map = e->value_map();
            size_t removed = 0;
            for (auto& key : keys) {
                if (map.erase(key)) {
                    ++ removed;
                }
            }
            if (map.empty()) {
                current_store().erase(rk);
            }
            return reply_builder::build(removed);
        });
    });
}


future<scattered_message_ptr> database::hdel(const redis_key& rk, sstring& key)
{
    return with_allocator(allocator(), [this, &rk, &key] {
        return current_store().with_entry_run(rk, [this, &rk, &key] (cache_entry* e) {
            if (!e) {
                return reply_builder::build(msg_zero);
            }
            if (e->type_of_map() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& map = e->value_map();
            bool exists = map.erase(key);
            if (map.empty()) {
                current_store().erase(rk);
            }
            return reply_builder::build(exists ? msg_ok : msg_err);
        });
    });
}

future<scattered_message_ptr> database::hexists(const redis_key& rk, sstring& key)
{
    return with_allocator(allocator(), [this, &rk, &key] {
        return current_store().with_entry_run(rk, [this, &key] (cache_entry* e) {
            if (!e) {
                return reply_builder::build(msg_zero);
            }
            if (e->type_of_map() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& map = e->value_map();
            auto result = map.exists(key);
            return reply_builder::build(result ? msg_one : msg_zero);
        });
    });
}

future<scattered_message_ptr> database::hstrlen(const redis_key& rk, sstring& key)
{
    return current_store().with_entry_run(rk, [this, &key] (cache_entry* e) {
        if (!e) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_map() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& map = e->value_map();
        return map.with_entry_run(key, [] (const dict_entry* d) {
            if (!d) {
                return reply_builder::build(msg_zero);
            }
            return reply_builder::build(d->value_bytes_size());
        });
    });
}

future<scattered_message_ptr> database::hlen(const redis_key& rk)
{
    return current_store().with_entry_run(rk, [this] (cache_entry* e) {
        if (!e) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_map() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& map = e->value_map();
        return reply_builder::build(map.size());
    });
}

future<scattered_message_ptr> database::hgetall(const redis_key& rk)
{
    return hgetall_impl<true, true>(rk);
}

future<scattered_message_ptr> database::hgetall_values(const redis_key& rk)
{
    return hgetall_impl<false, true>(rk);
}

future<scattered_message_ptr> database::hgetall_keys(const redis_key& rk)
{
    return hgetall_impl<true, false>(rk);
}


future<scattered_message_ptr> database::hmget(const redis_key& rk, std::vector<sstring>& keys)
{
    return current_store().with_entry_run(rk, [this, &keys] (const cache_entry* e) {
        if (!e) {
            return reply_builder::build(msg_err);
        }
        if (e->type_of_map() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& map = e->value_map();
        std::vector<const dict_entry*> entries;
        map.fetch(keys, entries);
        return reply_builder::build<false, true>(entries);
    });
}

future<scattered_message_ptr> database::srandmember(const redis_key& rk, size_t count)
{
     return current_store().with_entry_run(rk, [&count] (const cache_entry* e) {
        std::vector<const dict_entry*> result;
        if (!e) {
            return reply_builder::build<true, false>(result);
        }
        if (e->type_of_set() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& set = e->value_set();
        count = std::min(count, set.size());
        for (size_t i = 0; i < count; ++i) {
            auto index = rand_generater::rand_less_than(set.size());
            auto entry = set.at(index);
            result.push_back(&(*entry));
        }
        return reply_builder::build<true, false>(result);
     });
}

future<scattered_message_ptr> database::sadds(const redis_key& rk, std::vector<sstring>& members)
{
    return with_allocator(allocator(), [this, &rk, &members] {
        return current_store().with_entry_run(rk, [this, &rk, &members] (cache_entry* e) {
            auto o = e;
            if (!o) {
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::set_initializer());
                current_store().insert(entry);
                o = entry;
            }
            if (o->type_of_set() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& set = o->value_set();
            size_t inserted = 0;
            for (auto& member : members) {
                if (set.insert(current_allocator().construct<dict_entry>(member))) {
                    inserted++;
                }
            }
            return reply_builder::build(inserted);
        });
    });
}

bool database::sadd_direct(const redis_key& rk, sstring& member)
{
    return with_allocator(allocator(), [this, &rk, &member] {
        return current_store().with_entry_run(rk, [this, &rk, &member] (cache_entry* e) {
            auto o = e;
            if (!o) {
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::set_initializer());
                current_store().insert(entry);
                o = entry;
            }
            if (o->type_of_set() == false) {
                return false;
            }
            auto& set = o->value_set();
            set.insert(current_allocator().construct<dict_entry>(member));
            return true;
        });
    });
}

bool database::sadds_direct(const redis_key& rk, std::vector<sstring>& members)
{
    return with_allocator(allocator(), [this, &rk, &members] {
        return current_store().with_entry_run(rk, [this, &rk, &members] (cache_entry* e) {
            auto o = e;
            if (!o) {
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::set_initializer());
                current_store().insert(entry);
                o = entry;
            }
            if (o->type_of_set() == false) {
                return false;
            }
            auto& set = o->value_set();
            size_t inserted = 0;
            for (auto& member : members) {
                if (set.insert(current_allocator().construct<dict_entry>(member))) {
                    inserted++;
                }
            }
            return true;
        });
    });
}

future<scattered_message_ptr> database::scard(const redis_key& rk)
{
     return current_store().with_entry_run(rk, [] (const cache_entry* e) {
        if (!e) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_set() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& set = e->value_set();
        return reply_builder::build(set.size());
     });
}

future<scattered_message_ptr> database::sismember(const redis_key& rk, sstring& member)
{
     return current_store().with_entry_run(rk, [&member] (const cache_entry* e) {
        if (!e) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_set() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& set = e->value_set();
        auto result = set.exists(member);
        return reply_builder::build(result ? msg_one : msg_zero);
     });
}

future<scattered_message_ptr> database::smembers(const redis_key& rk)
{
    return current_store().with_entry_run(rk, [] (const cache_entry* e) {
        if (!e) {
            return reply_builder::build(msg_nil);
        }
        if (e->type_of_set() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& set = e->value_set();
        std::vector<const dict_entry*> entries;
        set.fetch(entries);
        return reply_builder::build<true, false>(entries);
    });
}

future<foreign_ptr<lw_shared_ptr<sstring>>> database::get_hll_direct(const redis_key& rk)
{
    using return_type = foreign_ptr<lw_shared_ptr<sstring>>;
    return current_store().with_entry_run(rk, [] (const cache_entry* e) {
        if (!e || e->type_of_hll() == false) {
            return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<sstring>>(nullptr));
        }
        auto data = e->value_bytes_data();
        auto size = e->value_bytes_size();
        return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<sstring>>(make_lw_shared<sstring>(sstring {data, size})));
    });
}

future<foreign_ptr<lw_shared_ptr<std::vector<sstring>>>> database::smembers_direct(const redis_key& rk)
{
    using result_type = std::vector<sstring>;
    using return_type = foreign_ptr<lw_shared_ptr<result_type>>;
    return current_store().with_entry_run(rk, [] (const cache_entry* e) {
        if (!e || e->type_of_set() == false) {
            return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<result_type>>(make_lw_shared<result_type>(result_type {})));
        }
        auto& set = e->value_set();
        result_type keys;
        set.fetch_keys(keys);
        return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<result_type>>(make_lw_shared<result_type>(std::move(keys))));
    });
}

future<scattered_message_ptr> database::spop(const redis_key& rk, size_t count)
{
    return with_allocator(allocator(), [this, &rk, &count] {
        return current_store().with_entry_run(rk, [this, &rk, &count] (cache_entry* e) {
            if (!e) {
                return reply_builder::build(msg_nil);
            }
            if (e->type_of_set() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& set = e->value_set();
            std::vector<dict_lsa::const_iterator> removed;
            std::vector<const dict_entry*> entries;
            count = std::min(count, set.size());
            for (size_t i = 0; i < count; ++i) {
                auto index = rand_generater::rand_less_than(set.size());
                auto entry = set.at(index);
                entries.push_back(&(*entry));
                removed.push_back(entry);
            }
            auto reply = reply_builder::build<true, false>(entries);
            if (!removed.empty()) {
                for (auto it : removed) {
                    set.erase(it);
                }
                if (set.empty()) {
                    current_store().erase(rk);
                }
            }
            return reply;
        });
    });
}

future<scattered_message_ptr> database::srem(const redis_key& rk, sstring& member)
{
    return with_allocator(allocator(), [this, &rk, &member] {
        return current_store().with_entry_run(rk, [this, &rk, &member] (cache_entry* e) {
            if (!e) {
                return reply_builder::build(msg_zero);
            }
            if (e->type_of_set() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& set = e->value_set();
            auto result = set.erase(member);
            if (set.empty()) {
                current_store().erase(rk);
            }
            return reply_builder::build(result ? msg_one : msg_zero);
        });
    });
}
bool database::srem_direct(const redis_key& rk, sstring& member)
{
    return with_allocator(allocator(), [this, &rk, &member] {
        return current_store().with_entry_run(rk, [this, &rk, &member] (cache_entry* e) {
            if (!e) {
                return true;
            }
            if (e->type_of_set() == false) {
                return false;
            }
            auto& set = e->value_set();
            auto result = set.erase(member);
            if (set.empty()) {
                current_store().erase(rk);
            }
            return result;
        });
    });
}

future<scattered_message_ptr> database::srems(const redis_key& rk, std::vector<sstring>& members)
{
    return with_allocator(allocator(), [this, &rk, &members] {
        return current_store().with_entry_run(rk, [this, &rk, &members] (cache_entry* e) {
            if (!e) {
                return reply_builder::build(msg_zero);
            }
            if (e->type_of_set() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& set = e->value_set();
            size_t removed = 0;
            for (auto& member : members) {
                if (set.erase(member)) {
                    removed++;
                }
            }
            if (set.empty()) {
                current_store().erase(rk);
            }
            return reply_builder::build(removed);
        });
    });
}

future<scattered_message_ptr> database::pttl(const redis_key& rk)
{
    return reply_builder::build(msg_nil);
}

future<scattered_message_ptr> database::ttl(const redis_key& rk)
{
    return reply_builder::build(msg_nil);
}

future<scattered_message_ptr> database::zadds(const redis_key& rk, std::unordered_map<sstring, double>& members, int flags)
{
    return with_allocator(allocator(), [this, &rk, &members, flags] {
        return current_store().with_entry_run(rk, [this, &rk, &members, flags] (cache_entry* e) {
            auto o = e;
            if (o == nullptr) {
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::sset_initializer());
                current_store().insert(entry);
                o = entry;
            }
            if (o->type_of_sset() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& sset = o->value_sset();
            size_t inserted = 0;
            if (flags & ZADD_NX) {
                inserted = sset.insert_if_not_exists(members);
            }
            else if (flags & ZADD_XX) {
                inserted = sset.update_if_only_exists(members);
            }
            else if (flags & ZADD_CH) {
                inserted = sset.insert_or_update(members);
            }
            else {
                assert(false);
            }
            return reply_builder::build(inserted);
        });
    });
}

bool database::zadds_direct(const redis_key& rk, std::unordered_map<sstring, double>& members, int flags)
{
    return with_allocator(allocator(), [this, &rk, &members, flags] {
        return current_store().with_entry_run(rk, [this, &rk, &members, flags] (cache_entry* e) {
            auto o = e;
            if (o == nullptr) {
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::sset_initializer());
                current_store().insert(entry);
                o = entry;
            }
            if (o->type_of_sset() == false) {
                return false;
            }
            auto& sset = o->value_sset();
            size_t inserted = 0;
            if (flags & ZADD_NX) {
                inserted = sset.insert_if_not_exists(members);
            }
            else if (flags & ZADD_XX) {
                inserted = sset.update_if_only_exists(members);
            }
            else if (flags & ZADD_CH) {
                inserted = sset.insert_or_update(members);
            }
            else {
                assert(false);
            }
            return inserted > 0;
        });
    });
}


future<scattered_message_ptr> database::zcard(const redis_key& rk)
{
    return current_store().with_entry_run(rk, [] (const cache_entry* e) {
        if (e == nullptr) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_sset() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& sset = e->value_sset();
        std::cout << " size: " << sset.size();
        return reply_builder::build(sset.size());
    });
}

future<scattered_message_ptr> database::zrem(const redis_key& rk, std::vector<sstring>& members)
{
    return with_allocator(allocator(), [this, &rk, &members] {
        return current_store().with_entry_run(rk, [this, &rk, &members] (cache_entry* e) {
            if (e == nullptr) {
                return reply_builder::build(msg_zero);
            }
            if (e->type_of_sset() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& sset = e->value_sset();
            auto removed = sset.erase(members);
            if (sset.empty()) {
               current_store().erase(rk);
            }
            return reply_builder::build(removed);
        });
    });
}

future<scattered_message_ptr> database::zcount(const redis_key& rk, double min, double max)
{
    return current_store().with_entry_run(rk, [min, max] (const cache_entry* e) {
        if (e == nullptr) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_sset() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& sset = e->value_sset();
        auto count = sset.count_by_score(min, max);
        return reply_builder::build(count);
    });
}

future<scattered_message_ptr> database::zincrby(const redis_key& rk, sstring& member, double delta)
{
    return with_allocator(allocator(), [this, &rk, &member, delta] {
        return current_store().with_entry_run(rk, [this, &rk, &member, delta] (cache_entry* e) {
            auto o = e;
            if (o == nullptr) {
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::sset_initializer());
                current_store().insert(entry);
                o = entry;
            }
            if (o->type_of_sset() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& sset = o->value_sset();
            auto result = sset.insert_or_update(member, delta);
            return reply_builder::build(result);
        });
    });
}

future<foreign_ptr<lw_shared_ptr<std::vector<std::pair<sstring, double>>>>> database::zrange_direct(const redis_key& rk, long begin, long end)
{
    using return_type = foreign_ptr<lw_shared_ptr<std::vector<std::pair<sstring, double>>>>;
    using result_type = std::vector<std::pair<sstring, double>>;
    return current_store().with_entry_run(rk, [begin, end] (const cache_entry* e) {
        if (e == nullptr || e->type_of_sset() == false) {
            return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<result_type>>(make_lw_shared<result_type>(result_type {})));
        }
        auto& sset = e->value_sset();
        result_type entries {};
        sset.fetch_by_rank(begin, end, entries);
        return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<result_type>>(make_lw_shared<result_type>(std::move(entries))));
    });
}

future<scattered_message_ptr> database::zrange(const redis_key& rk, long begin, long end, bool reverse, bool with_score)
{
    return current_store().with_entry_run(rk, [begin, end, reverse, with_score] (const cache_entry* e) {
        std::vector<const sset_entry*> entries;
        if (e == nullptr) {
            return reply_builder::build(entries, with_score);
        }
        if (e->type_of_sset() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& sset = e->value_sset();
        sset.fetch_by_rank(begin, end, entries);
        if (reverse) {
           std::reverse(std::begin(entries), std::end(entries));
        }
        return reply_builder::build(entries, with_score);
    });
}

future<scattered_message_ptr> database::zrangebyscore(const redis_key& rk, double min, double max, bool reverse, bool with_score)
{
    return current_store().with_entry_run(rk, [min, max, reverse, with_score] (const cache_entry* e) {
        std::vector<const sset_entry*> entries;
        if (e == nullptr) {
            return reply_builder::build(entries, with_score);
        }
        if (e->type_of_sset() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& sset = e->value_sset();
        sset.fetch_by_score(min, max, entries);
        if (reverse) {
           std::reverse(std::begin(entries), std::end(entries));
        }
        return reply_builder::build(entries, with_score);
    });
}

future<scattered_message_ptr> database::zrank(const redis_key& rk, sstring& member, bool reverse)
{
    return current_store().with_entry_run(rk, [this, &member, reverse] (const cache_entry* e) {
        if (e == nullptr) {
            return reply_builder::build(msg_nil);
        }
        if (e->type_of_sset() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& sset = e->value_sset();
        auto rank_opt = sset.rank(member);
        if (rank_opt) {
           auto rank = *rank_opt;
           if (reverse) {
               rank = sset.size() - rank;
           }
           return reply_builder::build(rank);
        }
        return reply_builder::build(msg_nil);
    });
}

future<scattered_message_ptr> database::zscore(const redis_key& rk, sstring& member)
{
    return current_store().with_entry_run(rk, [&member] (const cache_entry* e) {
        if (e == nullptr) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_sset() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& sset = e->value_sset();
        auto score_opt = sset.score(member);
        if (score_opt) {
           return reply_builder::build(*score_opt);
        }
        return reply_builder::build(msg_err);
    });
}

future<scattered_message_ptr> database::zremrangebyscore(const redis_key& rk, double min, double max)
{
    return with_allocator(allocator(), [this, &rk, min, max] {
        return current_store().with_entry_run(rk, [this, &rk, min, max] (cache_entry* e) {
            if (e == nullptr) {
                return reply_builder::build(msg_zero);
            }
            if (e->type_of_sset() == false) {
                return reply_builder::build(msg_type_err);
            }
            std::vector<const sset_entry*> entries;
            auto& sset = e->value_sset();
            sset.fetch_by_score(min, max, entries);
            auto removed = sset.erase(entries);
            if (sset.empty()) {
                current_store().erase(rk);
            }
            return reply_builder::build(removed);
        });
    });
}

future<scattered_message_ptr> database::zremrangebyrank(const redis_key& rk, size_t begin, size_t end)
{
    return with_allocator(allocator(), [this, &rk, begin, end] {
        return current_store().with_entry_run(rk, [this, &rk, begin, end] (cache_entry* e) {
            if (e == nullptr) {
                return reply_builder::build(msg_zero);
            }
            if (e->type_of_sset() == false) {
                return reply_builder::build(msg_type_err);
            }
            std::vector<const sset_entry*> entries;
            auto& sset = e->value_sset();
            sset.fetch_by_rank(begin, end, entries);
            auto removed = sset.erase(entries);
            if (sset.empty()) {
                current_store().erase(rk);
            }
            return reply_builder::build(removed);
        });
    });
}

bool database::select(size_t index)
{
    if (index > DEFAULT_DB_COUNT) {
        return false;
    }
    current_store_index = index;
    return true;
}


future<scattered_message_ptr> database::geodist(const redis_key& rk, sstring& lpos, sstring& rpos, int flag)
{
    double factor = 1;
    if (flag & GEODIST_UNIT_M) {
        factor = 1;
    }
    else if (flag & GEODIST_UNIT_KM) {
        factor = 1000;
    }
    else if (flag & GEODIST_UNIT_MI) {
        factor = 0.3048;
    }
    else if (flag & GEODIST_UNIT_FT) {
        factor = 1609.34;
    }
    return current_store().with_entry_run(rk, [this, &lpos, &rpos, factor] (const cache_entry* e) {
        if (e == nullptr) {
           return reply_builder::build(msg_err);
        }
        if (e->type_of_sset() == false) {
           return reply_builder::build(msg_type_err);
        }
        auto& sset = e->value_sset();
        auto l_score_opt = sset.score(lpos);
        auto r_score_opt = sset.score(rpos);
        if (!l_score_opt || !r_score_opt) {
           return reply_builder::build(msg_err);
        }
        double dist = 0;
        if (geo::dist(*l_score_opt, *r_score_opt, dist)) {
           return reply_builder::build(dist / factor);
        }
        else {
           return reply_builder::build(msg_err);
        }
    });
}

future<scattered_message_ptr> database::geohash(const redis_key& rk, std::vector<sstring>& members)
{
    return current_store().with_entry_run(rk, [this, members] (const cache_entry* e) {
        if (e == nullptr) {
           return reply_builder::build(msg_err);
        }
        if (e->type_of_sset() == false) {
           return reply_builder::build(msg_type_err);
        }
        auto& sset = e->value_sset();
        std::vector<const sset_entry*> entries;
        sset.fetch_by_key(members, entries);
        std::vector<sstring> geohash_set;
        for (size_t i = 0; i < entries.size(); ++i) {
            auto entry = entries[i];
            sstring hashstr;
            if (geo::encode_to_geohash_string(entry->score(), hashstr) == false) {
                return reply_builder::build(msg_err);
            }
            geohash_set.emplace_back(std::move(hashstr));;
        }
        return reply_builder::build(geohash_set);
    });
}

future<scattered_message_ptr> database::geopos(const redis_key& rk, std::vector<sstring>& members)
{
    return current_store().with_entry_run(rk, [this, members] (const cache_entry* e) {
        if (e == nullptr) {
           return reply_builder::build(msg_err);
        }
        if (e->type_of_sset() == false) {
           return reply_builder::build(msg_type_err);
        }
        auto& sset = e->value_sset();
        std::vector<const sset_entry*> entries;
        sset.fetch_by_key(members, entries);
        std::vector<sstring> geohash_set;
        for (size_t i = 0; i < entries.size(); ++i) {
            auto entry = entries[i];
            sstring hashstr;
            if (geo::encode_to_geohash_string(entry->score(), hashstr) == false) {
                return reply_builder::build(msg_err);
            }
            geohash_set.emplace_back(std::move(hashstr));;
        }
        return reply_builder::build(geohash_set);
    });
}
using georadius_result_type = std::pair<std::vector<std::tuple<sstring, double, double, double, double>>, int>;
future<foreign_ptr<lw_shared_ptr<georadius_result_type>>> database::georadius_coord_direct(const redis_key& rk, double longitude, double latitude, double radius, size_t count, int flag)
{
    using return_type = foreign_ptr<lw_shared_ptr<georadius_result_type>>;
    return current_store().with_entry_run(rk, [this, longitude, latitude, radius, count, flag] (const cache_entry* e) {
        if (e == nullptr) {
            return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {{}, REDIS_ERR})));
        }
        if (e->type_of_sset() == false) {
            return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {{}, REDIS_WRONG_TYPE})));
        }
        auto& sset = e->value_sset();
        return georadius(sset, longitude, latitude, radius, count, flag);
    });
}

future<foreign_ptr<lw_shared_ptr<georadius_result_type>>> database::georadius_member_direct(const redis_key& rk, sstring& pos, double radius, size_t count, int flag)
{

    using return_type = foreign_ptr<lw_shared_ptr<georadius_result_type>>;
    return current_store().with_entry_run(rk, [this, pos, radius, count, flag] (const cache_entry* e) {
        if (e == nullptr) {
            return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {{}, REDIS_ERR})));
        }
        if (e->type_of_sset() == false) {
            return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {{}, REDIS_WRONG_TYPE})));
        }
        auto& sset = e->value_sset();
        auto score_opt = sset.score(pos);
        if (!score_opt) {
            return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {{}, REDIS_ERR})));
        }
        double longitude = 0, latitude = 0;
        if (geo::decode_from_geohash(*score_opt, longitude, latitude) == false) {
            return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {{}, REDIS_ERR})));
        }
        return georadius(sset, longitude, latitude, radius, count, flag);
    });
}

future<foreign_ptr<lw_shared_ptr<georadius_result_type>>> database::georadius(const sset_lsa& sset, double longitude, double latitude, double radius, size_t count, int flag)
{
    using return_type = foreign_ptr<lw_shared_ptr<georadius_result_type>>;
    using data_type = std::vector<std::tuple<sstring, double, double, double, double>>;
    data_type points;
    auto fetch_point = [&sset, count] (double min, double max, double log, double lat, double r, data_type& points) -> size_t {
        std::vector<const sset_entry*> entries;
        sset.fetch_by_score(min, max, entries, count);
        size_t _count = 0;
        for (size_t i = 0; i < entries.size(); ++i) {
            auto e = entries[i];
            double score = e->score(), longitude = 0, latitude = 0, dist = 0;
            if (geo::decode_from_geohash(score, longitude, latitude) == false) {
                continue;
            }
            if (geo::dist(log, lat, longitude, latitude, dist) == false) {
                continue;
            }
            if (dist < r) {
                _count++;
                sstring n(e->key_data(), e->key_size());
                points.emplace_back(std::move(std::tuple<sstring, double, double, double, double>{std::move(n), score, dist, longitude, latitude}));
            }
        }
        return _count;
    };
    if (geo::fetch_points_from_location(longitude, latitude, radius, std::move(fetch_point), points) == false) {
        return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {{}, REDIS_ERR})));
    }
    if (flag & GEORADIUS_ASC) {
        std::sort(points.begin(), points.end(), [] (const auto& l, const auto& r) { return std::get<2>(l) > std::get<2>(r); });
    }
    else if (flag & GEORADIUS_DESC) {
        std::sort(points.begin(), points.end(), [] (const auto& l, const auto& r) { return std::get<2>(l) < std::get<2>(r); });
    }
    return make_ready_future<return_type>(foreign_ptr<lw_shared_ptr<georadius_result_type>>(make_lw_shared<georadius_result_type>(georadius_result_type {std::move(points), REDIS_OK})));
}

future<scattered_message_ptr> database::setbit(const redis_key& rk, size_t offset, bool value)
{
    return with_allocator(allocator(), [this, &rk, offset, value] {
        return current_store().with_entry_run(rk, [this, &rk, offset, value] (cache_entry* e) {
            auto o = e;
            size_t offset_in_bytes = offset >> 3;
            if (o == nullptr) {
               size_t offset_in_bytes = offset >> 3;
               size_t origin_size = offset_in_bytes + offset_in_bytes / 4;
               if (origin_size < 15) {
                   origin_size = 15;
               }
               auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), origin_size);
               current_store().insert(entry);
               o = entry;
            }
            if (o->type_of_bytes() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& mbytes = o->value_bytes();
            if (mbytes.size() < offset_in_bytes) {
                auto extend_size = offset_in_bytes + offset_in_bytes / 4;
                mbytes.extend(extend_size, 0);
            }
            auto result = bits_operation::set(mbytes, offset, value);
            return reply_builder::build(result ? msg_one : msg_zero);
        });
    });
}

future<scattered_message_ptr> database::getbit(const redis_key& rk, size_t offset)
{
    return current_store().with_entry_run(rk, [offset] (const cache_entry* e) {
        if (e == nullptr) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_bytes() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& mbytes = e->value_bytes();
        auto result = bits_operation::get(mbytes, offset);
        return reply_builder::build(result ? msg_one : msg_zero);
    });
}

future<scattered_message_ptr> database::bitcount(const redis_key& rk, long start, long end)
{
    return current_store().with_entry_run(rk, [start, end] (const cache_entry* e) {
        if (e == nullptr) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_bytes() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& mbytes = e->value_bytes();
        auto result = bits_operation::count(mbytes, start, end);
        return reply_builder::build(result);
    });
}

future<scattered_message_ptr> database::bitpos(const redis_key& rk, bool bit, long start, long end)
{
    return reply_builder::build(msg_nil);
}

future<scattered_message_ptr> database::pfadd(const redis_key& rk, std::vector<sstring>& elements)
{
    return with_allocator(allocator(), [this, &rk, &elements] {
        return current_store().with_entry_run(rk, [this, &rk, &elements] (cache_entry* e) {
            if (e == nullptr) {
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::hll_initializer());
                current_store().insert(entry);
                e = entry;
            }
            if (e->type_of_hll() == false) {
               return reply_builder::build(msg_type_err);
            }
            managed_bytes& mbytes = e->value_bytes();
            auto result = hll::append(mbytes, elements);
            return reply_builder::build(result);
        });
    });
}

future<scattered_message_ptr> database::pfcount(const redis_key& rk)
{
    return current_store().with_entry_run(rk, [] (cache_entry* e) {
        if (e == nullptr) {
            return reply_builder::build(msg_zero);
        }
        if (e->type_of_hll() == false) {
            return reply_builder::build(msg_type_err);
        }
        auto& mbytes = e->value_bytes();
        auto result = hll::count(mbytes);
        return reply_builder::build(result);
    });
}

future<scattered_message_ptr> database::pfmerge(const redis_key& rk, uint8_t* merged_sources, size_t size)
{
    return with_allocator(allocator(), [this, &rk, merged_sources, size] {
        return current_store().with_entry_run(rk, [this, &rk, merged_sources, size] (cache_entry* e) {
            if (e == nullptr) {
                auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), cache_entry::hll_initializer());
                current_store().insert(entry);
                e = entry;
            }
            if (e->type_of_hll() == false) {
                return reply_builder::build(msg_type_err);
            }
            auto& mbytes = e->value_bytes();
            hll::merge(mbytes, merged_sources, size);
            return reply_builder::build(msg_ok);
        });
    });
}


future<> database::stop()
{
    return make_ready_future<>();
}
}
