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
#include "list.hh"
namespace redis {
using item_ptr = foreign_ptr<lw_shared_ptr<item>>;
class list_storage : public storage {
public:
    list_storage(const sstring& name, dict* store) : storage(name, store)
    {
    }
    virtual ~list_storage()
    {
    }
    struct stats {
        uint64_t list_count_ = 0;
        uint64_t list_node_count_ = 0;
    };

    template<typename origin = local_origin_tag>
    int push(sstring& key, sstring& value, bool force, bool left)
    {
        redis_key rk{key};
        list* l = fetch_list(rk);
        if (l == nullptr) {
            if (!force) {
                return -1;
            }
            l = new list();
            auto list_item = item::create(key, l, REDIS_LIST);
            if (_store->set(rk, list_item) != 0) {
                return -1;
            }
        }
        auto new_item = item::create(origin::move_if_local(value));
        return (left ? l->add_head(new_item) : l->add_tail(new_item)) == 0 ? static_cast<int>(l->size()) : 0;
    }

    item_ptr pop(sstring& key, bool left)
    {
        redis_key rk{key};
        list* l = fetch_list(rk);
        if (l != nullptr) {
            auto it = left ? l->pop_head() : l->pop_tail();
            if (l->size() == 0) {
                _store->remove(rk);
            }
            return it;
        } 
        return nullptr;
    }

    int llen(sstring& key)
    {
        redis_key rk{key};
        list* l = fetch_list(rk);
        if (l != nullptr) {
            return l->size();
        } 
        return 0;
    }

    item_ptr lindex(sstring& key, int idx)
    {
        redis_key rk{key};
        list* l = fetch_list(rk);
        if (l != nullptr) {
            return l->index(idx);
        } 
        return nullptr;
    }
  
    template<typename origin = local_origin_tag>
    int linsert(sstring& key, sstring& pivot, sstring& value, bool after)
    {
        redis_key rk{key};
        list* l = fetch_list(rk);
        if (l != nullptr) {
            auto new_item = item::create(origin::move_if_local(value));
            return (after ? l->insert_after(pivot, new_item) : l->insert_before(pivot, new_item)) == 0 ? 1 : 0;
        } 
        return 0;
    }

    std::vector<item_ptr> lrange(sstring& key, int start, int end)
    {
        redis_key rk{key};
        list* l = fetch_list(rk);
        std::vector<item_ptr> result;
        if (l != nullptr) {
            return l->range(start, end);
        } 
        return std::move(result); 
    }

    template<typename origin = local_origin_tag>
    int lset(sstring& key, int idx, sstring& value)
    {
        redis_key rk{key};
        list* l = fetch_list(rk);
        if (l != nullptr) {
            auto new_item = item::create(origin::move_if_local(value));
            if (l->set(idx, new_item) == REDIS_OK) {
                return 1;
            }
        } 
        return 0;
    }

    int lrem(sstring& key, int count, sstring& value)
    {
        redis_key rk{key};
        list* l = fetch_list(rk);
        if (l != nullptr) {
            auto result = l->trem(count, value);
            if (l->size() == 0) {
                _store->remove(rk);
            }
            return result;
        }
        return 0;
    }

    int ltrim(sstring& key, int start, int end)
    {
        redis_key rk{key};
        list* l = fetch_list(rk);
        if (l != nullptr) {
            return l->trim(start, end);
        }
        return 0;
    }
protected:
  inline list* fetch_list(const redis_key& key)
  {
      auto it = _store->fetch_raw(key);
      if (it && it->type() == REDIS_LIST) {
          return it->list_ptr();
      }
      return nullptr;
  }
  stats _stats;
};

}
