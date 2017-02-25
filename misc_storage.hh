#pragma once
#include "storage.hh"
#include "base.hh"

namespace redis {
using item_ptr = foreign_ptr<lw_shared_ptr<item>>;
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
    std::pair<int64_t, int> counter_by(sstring& key, int64_t step, bool incr)
    {
        using result_type = std::pair<int64_t, int>;
        redis_key rk{key};
        auto it = _store->fetch_raw(rk);
        int64_t new_value = incr ? step : -step;
        if (it) {
            if (it->type() != REDIS_RAW_UINT64) {
                return result_type {0, REDIS_WRONG_TYPE};
            }
            new_value += it->int64();
        }
        auto new_item = item::create(key, new_value);
        if (_store->replace(rk, new_item) == REDIS_OK) {
            return result_type {new_value, REDIS_OK};
        }
        else {
            return result_type {0, REDIS_ERR};
        }
    }

    /** STRING API **/
    template <typename origin = local_origin_tag>
    int set(const redis_key& rk, sstring& val, long expire, uint32_t flag)
    {
        _store->remove(rk);
        auto new_item = item::create(rk, origin::move_if_local(val));
        return _store->set(rk, new_item);
    }

    template <typename origin = local_origin_tag>
    int append(sstring& key, sstring& val)
    {
        redis_key rk{key};
        size_t current_size = -1;
        auto it = _store->fetch_raw(rk);
        if (it) {
            auto exist_val = it->value();
            current_size = exist_val.size() + val.size();
            auto new_item = item::create(key,
                    origin::move_if_local(exist_val),
                    origin::move_if_local(val));
            if (_store->replace(rk, new_item) != 0) {
                return -1;
            }
        }
        else {
            current_size = val.size();
            auto new_item = item::create(key, origin::move_if_local(val));
            if (_store->set(rk, new_item) != 0) {
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

    lw_shared_ptr<item> fetch_item(sstring& key)
    {
        redis_key rk{key};
        return _store->fetch_raw(rk);
    }

    int type(sstring& key)
    {
        redis_key rk{key};
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return REDIS_ERR;
        }
        return static_cast<int>(it->type());
    }

    long ttl(sstring& key)
    {
        redis_key rk{key};
        auto it = _store->fetch_raw(rk);
        if (!it) {
            return -2;
        }
        if (it->ever_expires() == false) {
            return -1;
        }
        auto duration = it->get_timeout() - clock_type::now(); 
        return static_cast<long>(std::chrono::duration_cast<std::chrono::milliseconds>(duration).count()); 
    }
protected:
    stats _stats;
};
}
