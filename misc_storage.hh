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
    uint64_t counter_by(redis_key& key, uint64_t step, bool incr)
    {
        auto it = _store->fetch_raw(key);
        if (it != nullptr) {
            if (it->type() != REDIS_RAW_UINT64) {
                return REDIS_ERR;
            }
            return it->incr(incr ? step : -step);
        } else {
            const size_t item_size = item::item_size_for_uint64(key.size());
            auto new_item = local_slab().create(item_size, key, step);
            intrusive_ptr_add_ref(new_item);
            if (_store->set(origin::move_if_local(key), new_item) != REDIS_OK)
                return REDIS_ERR;
            return step;
        }
    }

    /** STRING API **/
    template <typename origin = local_origin_tag>
    int set(redis_key& key, sstring& val, long expire, uint32_t flag)
    {
        _store->remove(key);
        const size_t item_size = item::item_size_for_string(key.size(), val.size());
        auto new_item = local_slab().create(item_size, key, origin::move_if_local(val));
        intrusive_ptr_add_ref(new_item);
        return _store->set(origin::move_if_local(key), new_item);
    }

    template <typename origin = local_origin_tag>
    int append(redis_key& key, sstring& val)
    {
        size_t current_size = -1;
        auto it = _store->fetch_raw(key);
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
            if (_store->replace(origin::move_if_local(key), new_item) != 0) {
                intrusive_ptr_release(new_item);
                return -1;
            }
        }
        else {
            current_size = val.size();
            const size_t item_size = item::item_size_for_string(key.size(), val.size());
            auto new_item = local_slab().create(item_size, key, origin::move_if_local(val));
            intrusive_ptr_add_ref(new_item);
            if (_store->set(origin::move_if_local(key), new_item) != 0) {
                intrusive_ptr_release(new_item);
                return -1;
            }
        }
        return current_size;
    }

    int del(redis_key& key)
    {
        return _store->remove(key) == REDIS_OK ? 1 : 0;
    }

    int exists(redis_key& key)
    {
        return _store->exists(key);
    }

    item_ptr get(redis_key& key)
    {
        auto a = _store->fetch(key);
        return a;
    }

    int strlen(redis_key& key)
    {
        auto i = _store->fetch(key);
        if (i) {
            return i->value_size();
        }
        return 0;
    }

    int expire(redis_key& key, long expired)
    {
      auto it = _store->fetch_raw(key);
      if (it == nullptr) {
        return REDIS_ERR;
      }
      //auto exp = expiration(get_wc_to_clock_type_delta(), expired);
      //it->update_expired_point(exp);
      return REDIS_OK;
    }
protected:
    stats _stats;
};
}
