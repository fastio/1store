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
    int zadds(sstring& key, std::unordered_map<sstring, double>& members)
    {
        redis_key rk {key};
        auto zset = fetch_zset(rk);
        if (zset == nullptr) {
            zset = new sorted_set();
            auto zset_item = item::create(key, zset, REDIS_ZSET);
            if (_store->set(key, zset_item) != 0) {
                return -1;
            }
        }
        int count = 0;
        for (auto& entry : members) {
            sstring key = entry.first;
            double  score = entry.second;
            redis_key member_data {std::ref(key), std::hash<sstring>()(key)};
            if (zset->exists(member_data) == false) {
                auto new_item = item::create(member_data, score);
                if (zset->insert(member_data, new_item)) {
                    count++;
                }
            }
        }
        return count;
    }

    template<typename origin = local_origin_tag>
    int zadd(sstring& key, sstring& member, double score)
    {
        redis_key rk {key};
        auto zset = fetch_zset(rk);
        if (zset == nullptr) {
            zset = new sorted_set();
            auto zset_item = item::create(key, zset, REDIS_ZSET);
            if (_store->set(key, zset_item) != 0) {
                return -1;
            }
        }
        redis_key member_data {std::ref(member), std::hash<sstring>()(member)};
        if (zset->exists(member_data) == false) {
            auto new_item = item::create(member_data, score);
            if (zset->insert(member_data, new_item)) {
                return 0;
            }
        }
        return -1;
    }

    std::vector<item_ptr> zrange(sstring& key, size_t begin, size_t end)
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
        return zset->range(begin, end);
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
