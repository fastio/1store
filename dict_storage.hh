#pragma once
#include "storage.hh"
#include "base.hh"
#include "dict.hh"
namespace redis {
class dict_storage : public storage {
public:
    dict_storage(const sstring& name, dict* store) : storage(name, store)
    {
    }
    virtual ~dict_storage()
    {
    }
    struct stats {
        uint64_t dict_count_ = 0;
        uint64_t dict_node_count_ = 0;
    };
    // HSET
    template<typename origin = local_origin_tag>
    int hset(const sstring& key, sstring& field, sstring& value)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d == nullptr) {
            const size_t dict_size = item::item_size_for_dict(key.size());
            d = new dict();
            auto dict_item = local_slab().create(dict_size, key, d, REDIS_DICT);
            intrusive_ptr_add_ref(dict_item);
            if (_store->set(rk, dict_item) != 0) {
                intrusive_ptr_release(dict_item);
                return -1;
            }
        }
        const size_t item_size = item::item_size_for_string(field.size(), value.size());
        auto field_hash = std::hash<sstring>()(field);
        redis_key field_key{std::ref(field), field_hash};
        auto new_item = local_slab().create(item_size, field_key, origin::move_if_local(value));
        intrusive_ptr_add_ref(new_item);
        return d->replace(field_key, new_item);
    }

    // HMSET
    template<typename origin = local_origin_tag>
    int hmset(const sstring& key, std::unordered_map<sstring, sstring>& kv)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d == nullptr) {
            const size_t dict_size = item::item_size_for_dict(key.size());
            d = new dict();
            auto dict_item = local_slab().create(dict_size, key, d, REDIS_DICT);
            intrusive_ptr_add_ref(dict_item);
            if (_store->set(rk, dict_item) != 0) {
                intrusive_ptr_release(dict_item);
                return -1;
            }
        }
        for (auto p = kv.begin(); p != kv.end(); p++) {
            sstring field = p->first;
            auto hash = std::hash<sstring>()(field);
            sstring& value = p->second;
            redis_key field_key{std::ref(field), std::move(hash)};
            const size_t item_size = item::item_size_for_string(field.size(), value.size());
            auto new_item = local_slab().create(item_size, field_key, origin::move_if_local(value));
            intrusive_ptr_add_ref(new_item);
            if (d->replace(field_key, new_item) != -1) {
                intrusive_ptr_release(new_item);
                return -1;
            }
        }
        return 0;
    }

    // HGET
    item_ptr hget(const sstring& key, sstring& field)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d != nullptr) {
            auto hash = std::hash<sstring>()(field);
            redis_key field_key{std::ref(field), hash};
            return d->fetch(field_key);
        }
        return nullptr;
    }

    // HDEL
    int hdel(const sstring& key, sstring& field)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d != nullptr) {
            auto hash = std::hash<sstring>()(field);
            redis_key field_key{std::ref(field), hash};
            return d->remove(field_key);
        }
        return 0;
    }

    // HEXISTS
    int hexists(const sstring& key, sstring& field)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d != nullptr) {
            auto hash = std::hash<sstring>()(field);
            redis_key field_key{std::ref(field), hash};
            return d->exists(field_key);
        }
        return REDIS_ERR;
    }

    // HSTRLEN
    int hstrlen(const sstring& key, sstring& field)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d != nullptr) {
            auto hash = std::hash<sstring>()(field);
            redis_key field_key{std::ref(field), hash};
            auto it = d->fetch(field_key);
            if (it && it->type() == REDIS_RAW_STRING) {
                return static_cast<int>(it->value_size());
            }
        }
        return 0;
    }

    // HLEN
    int hlen(const sstring& key)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d != nullptr) {
            return d->size();
        }
        return 0;
    }

    // HINCRBY
    template<typename origin = local_origin_tag>
    int hincrby(const sstring& key, sstring& field, int delta)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d == nullptr) {
            const size_t dict_size = item::item_size_for_dict(key.size());
            d = new dict();
            auto dict_item = local_slab().create(dict_size, key, d, REDIS_DICT);
            intrusive_ptr_add_ref(dict_item);
            if (_store->set(rk, dict_item) != 0) {
                intrusive_ptr_release(dict_item);
                return -1;
            }
        }
        auto hash = std::hash<sstring>()(field);
        redis_key field_key{std::ref(field), hash};
        auto it = d->fetch(field_key);
        if (!it) {
            const size_t item_size = item::item_size_for_int64(field.size());
            auto new_item = local_slab().create(item_size, field_key, static_cast<int64_t>(delta));
            intrusive_ptr_add_ref(new_item);
            if (d->set(field_key, new_item) == -1) {
                intrusive_ptr_release(new_item);
                return -1;
            }
            return delta;
        }
        if (it->type() == REDIS_RAW_INT64) {
            return it->incr(static_cast<int64_t>(delta));
        }
        return -1;
    }

    // HINCRBYFLOAT
    template<typename origin = local_origin_tag>
    double hincrbyfloat(const sstring& key, sstring& field, double delta)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d == nullptr) {
            const size_t dict_size = item::item_size_for_dict(key.size());
            d = new dict();
            auto dict_item = local_slab().create(dict_size, key, d, REDIS_DICT);
            intrusive_ptr_add_ref(dict_item);
            if (_store->set(rk, dict_item) != 0) {
                intrusive_ptr_release(dict_item);
                return -1;
            }
        }
        auto hash = std::hash<sstring>()(field);
        redis_key field_key{std::ref(field), hash};
        auto it = d->fetch(field_key);
        if (!it) {
            const size_t item_size = item::item_size_for_double(field.size());
            auto new_item = local_slab().create(item_size, field_key, delta);
            intrusive_ptr_add_ref(new_item);
            if (d->set(field_key, new_item) == -1) {
                intrusive_ptr_release(new_item);
                return -1;
            }
            return delta;
        }
        if (it->type() == REDIS_RAW_DOUBLE) {
            return it->incr(delta);
        }
        return -1;
    }

    // HGETALL
    std::vector<item_ptr> hgetall(const sstring& key)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d != nullptr) {
            return d->fetch();
        }
        std::vector<item_ptr> empty;
        return std::move(empty);
    }

    // HMGET
    std::vector<item_ptr> hmget(const sstring& key, std::unordered_set<sstring>& keys)
    {
        redis_key rk{key};
        dict* d = fetch_dict(rk);
        if (d != nullptr) {
            return d->fetch(keys);
        }
        std::vector<item_ptr> empty;
        return std::move(empty);
    }

protected:
    stats _stats;
    inline dict* fetch_dict(const redis_key& key)
    {
        auto it = _store->fetch_raw(key);
        if (it != nullptr && it->type() == REDIS_DICT) {
            return static_cast<dict*>(it->ptr());
        }
        return nullptr;
    }
};
}
