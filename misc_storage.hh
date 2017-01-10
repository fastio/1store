#pragma once
#include "storage.hh"
#include "base.hh"

namespace redis {
class misc_storage : public storage {
public:
    misc_storage(const sstring& name, dict* store) : storage(name, store)
    {
    }
    virtual ~misc_storage()
    {
    }
    struct stats {
        uint64_t key_count_;
    };

    /** COUNTER API **/
    template <typename origin = local_origin_tag>
    uint64_t counter_by(sstring& key, uint64_t step, bool incr)
    {
        redis_key rk{key};
        auto it = _store->fetch_raw(rk);
        if (it != nullptr) {
            if (it->type() != REDIS_RAW_UINT64) {
                return REDIS_ERR;
            }
            return it->incr(incr ? step : -step);
        } else {
            const size_t item_size = item::item_size_for_uint64(key.size());
            auto new_item = local_slab().create(item_size, key, step);
            intrusive_ptr_add_ref(new_item);
            if (_store->set(rk, new_item) != REDIS_OK)
                return REDIS_ERR;
            return step;
        }
    }

    /** STRING API **/
    template <typename origin = local_origin_tag>
    int set(sstring& key, sstring& val, long expire, uint32_t flag)
    {
        redis_key rk{key};
        _store->remove(key);
        const size_t item_size = item::item_size_for_string(key.size(), val.size());
        auto new_item = local_slab().create(item_size, key, origin::move_if_local(val));
        intrusive_ptr_add_ref(new_item);
        return _store->set(rk, new_item);
    }

    template <typename origin = local_origin_tag>
    int append(sstring& key, sstring& val)
    {
        redis_key rk{key};
        size_t current_size = -1;
        auto it = _store->fetch_raw(rk);
        if (it != nullptr) {
            auto exist_val = it->value();
            current_size = exist_val.size() + val.size();
            const size_t item_size = item::item_size_for_raw_string_append(key.size(), val.size(), exist_val.size());
            auto new_item = local_slab().create(item_size,
                    key,
                    origin::move_if_local(exist_val),
                    origin::move_if_local(val));
            intrusive_ptr_add_ref(new_item);
            intrusive_ptr_release(it);
            if (_store->replace(rk, new_item) != 0) {
                intrusive_ptr_release(new_item);
                return -1;
            }
        }
        else {
            current_size = val.size();
            const size_t item_size = item::item_size_for_string(key.size(), val.size());
            auto new_item = local_slab().create(item_size, key, origin::move_if_local(val));
            intrusive_ptr_add_ref(new_item);
            if (_store->set(rk, new_item) != 0) {
                intrusive_ptr_release(new_item);
                return -1;
            }
        }
        return current_size;
    }

    int del(sstring& key)
    {
        redis_key rk{key};
        return _store->remove(rk) == REDIS_OK ? 1 : 0;
    }

    int exists(sstring& key)
    {
        redis_key rk{key};
        return _store->exists(rk);
    }

    item_ptr get(sstring& key)
    {
        redis_key rk{key};
        return _store->fetch(rk);
    }

    int strlen(sstring& key)
    {
        redis_key rk{key};
        auto i = _store->fetch(rk);
        if (i) {
            return i->value_size();
        }
        return 0;
    }

    int expire(sstring& key, long expired)
    {
        redis_key rk{key};
      auto it = _store->fetch_raw(rk);
      if (it == nullptr) {
        return REDIS_ERR;
      }
      return REDIS_OK;
    }
protected:
    stats _stats;
};
}
