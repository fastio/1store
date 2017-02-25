/*
 * This file is open source software, licensed to you under the terms
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
 *
 *  Copyright (c) 2016-2026, Peng Jian, pstack@163.com. All rights reserved.
 *
 */
#pragma once
#include "storage.hh"
#include "base.hh"
#include "sorted_set.hh"
namespace redis {
using item_ptr = foreign_ptr<lw_shared_ptr<item>>;
class sorted_set_storage : public storage {
public:
    sorted_set_storage(const sstring& name, dict* store) : storage(name, store)
    {
    }
    virtual ~sorted_set_storage()
    {
    }
    template<typename origin = local_origin_tag>
    std::pair<size_t, int> zadds(sstring& key, std::unordered_map<sstring, double>&& members, int flags)
    {
        using result_type = std::pair<size_t, int>;
        redis_key rk {key};
        auto it = _store->fetch_raw(key);
        sorted_set* zset = nullptr; 
        if (!it) {
            zset = new sorted_set();
            auto zset_item = item::create(key, zset, REDIS_ZSET);
            if (_store->set(key, zset_item) != 0) {
                return result_type {0, REDIS_ERR};
            }
        }
        else {
            if (it->type() == REDIS_ZSET) {
                zset = it->zset_ptr();
            }
            else {
                return result_type {0, REDIS_WRONG_TYPE};
            }
        }
        size_t count = 0;
        for (auto& entry : members) {
            sstring key = entry.first;
            double  score = entry.second;
            redis_key member_data {std::ref(key), std::hash<sstring>()(key)};
            auto de = zset->fetch(member_data);
            if (de) {
                if (flags & ZADD_NX || score == 0) {
                    continue;
                }
                score += de->Double();
                auto new_item = item::create(member_data, score);
                if (zset->replace(member_data, new_item) == REDIS_OK) {
                    count++;
                }
            }
            else if (!(flags & ZADD_XX)) {
                auto new_item = item::create(member_data, score);
                if (zset->replace(member_data, new_item) == REDIS_OK) {
                    count++;
                }
            }
        }
        return result_type {count, REDIS_OK};
    }

    size_t zrem(sstring& key, std::vector<sstring>&& members)
    {
        redis_key rk {key};
        auto zset = fetch_zset(rk);
        if (zset != nullptr) {
            size_t count = 0;
            for (size_t i = 0; i < members.size(); ++i) {
                redis_key m{members[i]};
                if (zset->remove(m) == REDIS_OK) {
                    count++;
                }
            }
            if (zset->size() == 0) {
                _store->remove(rk);
            }
            return count;
        }
        return 0;
    }

    size_t zrank(sstring& key, sstring&& member, bool reverse)
    {
        redis_key rk {key};
        auto zset = fetch_zset(rk);
        if (zset == nullptr) {
            return -1;
        }
        redis_key m{member};
        return zset->rank(m, reverse);
    }

    std::vector<item_ptr> zrange(const sstring& key, long begin, long end, bool reverse)
    {
        redis_key rk {key};
        auto zset = fetch_zset(rk);
        if (zset == nullptr) {
            return std::vector<item_ptr>();
        }
        return zset->range_by_rank(begin, end, reverse);
    }

    std::vector<item_ptr> zrangebyscore(sstring& key, double min, double max, bool reverse)
    {
        redis_key rk {key};
        auto zset = fetch_zset(rk);
        if (zset == nullptr) {
            zset = new sorted_set();
            auto zset_item = item::create(key, zset, REDIS_ZSET);
            if (_store->set(key, zset_item) != 0) {
                return std::vector<item_ptr>();
            }
        }
        return zset->range_by_score(min, max, reverse);
    }

    size_t zcard(sstring& key)
    {
        redis_key rk {key};
        auto zset = fetch_zset(rk);
        if (zset == nullptr) {
            return -1;
        }
        return zset->size();
    }

    size_t zcount(sstring& key, double min, double max)
    {
        redis_key rk {key};
        auto zset = fetch_zset(rk);
        if (zset == nullptr) {
            return -1;
        }
        return zset->count(min, max);
    }

    template<typename origin = local_origin_tag>
    std::pair<double, bool> zincrby(sstring& key, sstring&& member, double delta)
    {
        using result_type = std::pair<double, bool>;
        redis_key rk {key};
        auto it = _store->fetch_raw(key);
        if (it && it->type() != REDIS_ZSET) {
            return result_type{0, false};
        }
        sorted_set* zset = nullptr;
        if (!it) {
            zset = new sorted_set();
            auto zset_item = item::create(rk, zset, REDIS_ZSET);
            if (_store->set(rk, zset_item) != REDIS_OK) {
                return result_type{0, false};
            }
        }
        else {
            zset = it->zset_ptr();
        }
        redis_key mk {member};
        double new_value = delta;
        auto exists_member = zset->fetch(mk);
        if (exists_member) {
            new_value += exists_member->Double();
        }
        auto new_member = item::create(mk, new_value);
        if (zset->replace(mk, new_member) == REDIS_OK) {
            return result_type {new_value, true};
        }
        else {
            return result_type {0, false}; 
        }
    }

    std::pair<double, bool> zscore(sstring& key, sstring&& member)
    {
        using result_type = std::pair<double, bool>;
        redis_key rk {key};
        auto it = _store->fetch_raw(key);
        if (!it || (it && it->type() != REDIS_ZSET)) {
            return result_type{0, false};
        }
        auto zset = it->zset_ptr();
        redis_key mk {member};
        auto value = zset->fetch(mk);
        if (!value) {
            return result_type{0, false};
        }
        return result_type{value->Double(), true};
    }
protected:
    inline sorted_set* fetch_zset(const redis_key& key)
    {
        auto it = _store->fetch_raw(key);
        if (it && it->type() == REDIS_ZSET) {
            return it->zset_ptr();
        }
        return nullptr;
    }
};
}
