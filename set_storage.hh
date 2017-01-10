#pragma once
#include "storage.hh"
#include "base.hh"
#include "dict.hh"
namespace redis {
class set_storage : public storage {
public:
    set_storage(const sstring& name, dict* store) : storage(name, store)
    {
    }
    virtual ~set_storage()
    {
    }
    struct stats {
        uint64_t set_count_ = 0;
        uint64_t set_node_count_ = 0;
    };
    template<typename origin = local_origin_tag>
    int sadds(sstring& key, std::vector<sstring>&& members)
    {
        redis_key rk{key};
        dict* d = fetch_set(key);
        if (d == nullptr) {
            const size_t dict_size = item::item_size_for_dict(key.size());
            d = new dict();
            auto dict_item = local_slab().create(dict_size, key, d, REDIS_SET);
            intrusive_ptr_add_ref(dict_item);
            if (_store->set(key, dict_item) != 0) {
                intrusive_ptr_release(dict_item);
                return -1;
            }
        }
        int count = 0;
        for (sstring& member : members) {
            const size_t item_size = item::item_size_for_raw_string(member.size());
            auto member_hash = std::hash<sstring>()(member);
            redis_key member_data {std::ref(member), member_hash};
            auto new_item = local_slab().create(item_size, member_data);
            intrusive_ptr_add_ref(new_item);
            if (d->replace(member_data, new_item)) {
                count++;
            }
        }
        return count;
    }
    template<typename origin = local_origin_tag>
    int sadd(sstring& key, sstring& member)
    {
        redis_key rk{key};
        dict* d = fetch_set(key);
        if (d == nullptr) {
            const size_t dict_size = item::item_size_for_dict(key.size());
            d = new dict();
            auto dict_item = local_slab().create(dict_size, key, d, REDIS_SET);
            intrusive_ptr_add_ref(dict_item);
            if (_store->set(key, dict_item) != 0) {
                intrusive_ptr_release(dict_item);
                return -1;
            }
        }
        const size_t item_size = item::item_size_for_raw_string(member.size());
        auto member_hash = std::hash<sstring>()(member);
        redis_key member_data {std::ref(member), member_hash};
        auto new_item = local_slab().create(item_size, member_data);
        intrusive_ptr_add_ref(new_item);
        if (d->replace(member_data, new_item)) {
            return 1;
        }
        return 0;
    }

    // SCARD
    template<typename origin = local_origin_tag>
    int scard(sstring& key)
    {
        redis_key rk{key}; 
        dict* d = fetch_set(rk);
        return d != nullptr ? d->size() : 0;
    }

    // SISMEMBER
    int sismember(sstring& key, sstring&& member)
    {
        redis_key rk{key};
        dict* d = fetch_set(key);
        if (d != nullptr) {
            auto member_hash = std::hash<sstring>()(member);
            redis_key member_data {std::ref(member), member_hash};
            return d->exists(member_data);
        }
        return 0;
    }

    // SMEMBERS
    std::vector<item_ptr> smembers(sstring& key)
    {
        redis_key rk{key};
        dict* d = fetch_set(rk);
        if (d == nullptr) {
            return std::vector<item_ptr>();
        }
        return d->fetch();
    }

    // SPOP
    item_ptr spop(sstring& key)
    {
        redis_key rk{key};
        dict* d = fetch_set(rk);
        if (d == nullptr) return nullptr;
        return d->random_fetch_and_remove();
    }

    // SRANDMEMBER
    item_ptr srandmember(sstring& key, sstring&& member)
    {
        redis_key rk{key};
        dict* d = fetch_set(rk);
        if (d != nullptr) {
            auto member_hash = std::hash<sstring>()(member);
            redis_key member_data {std::ref(member), std::ref(member_hash)};
            return d->fetch(member_data);
        }
        return nullptr;
    }

    // SREM
    int srem(sstring& key, sstring&& member)
    {
        redis_key rk{key};
        dict* d = fetch_set(key);
        if (d != nullptr) {
            auto member_hash = std::hash<sstring>()(member);
            redis_key member_data {std::ref(member), std::ref(member_hash)};
            return d->remove(member_data) == REDIS_OK ? 1 : 0;
        }
        return 0;
    }
    int srems(sstring& key, std::vector<sstring>&& members)
    {
        redis_key rk{key};
        dict* d = fetch_set(key);
        int count = 0;
        if (d != nullptr) {
            for (sstring& member : members) {
                auto member_hash = std::hash<sstring>()(member);
                redis_key member_data {std::ref(member), std::ref(member_hash)};
                if (d->remove(member_data) == REDIS_OK) {
                    count++;
                }
            }
        }
        return count;
    }

    int srem(sstring& key, sstring& member)
    {
        redis_key rk{key};
        dict* d = fetch_set(key);
        if (d != nullptr) {
            auto member_hash = std::hash<sstring>()(member);
            redis_key member_data {std::ref(member), std::ref(member_hash)};
            if (d->remove(member_data) == REDIS_OK) {
                return 1;
            }
        }
        return 0;
    }
protected:
    stats _stats;
    inline dict* fetch_set(const redis_key& key)
    {
        auto it = _store->fetch_raw(key);
        if (it != nullptr && it->type() == REDIS_SET) {
            return static_cast<dict*>(it->ptr());
        }
        return nullptr;
    }
};
}
