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
#include "core/stream.hh"
#include "core/memory.hh"
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include "core/shared_ptr.hh"
#include "core/sharded.hh"
#include "base.hh"
namespace redis {
class item;
class dict_iterator;
using item_ptr = foreign_ptr<boost::intrusive_ptr<item>>;
class dict : public object {
private:
    friend class dict_iterator;
    struct rep;
    rep* _rep;
public:
    dict();
    virtual ~dict();
    int set(const redis_key& key, item* val);
    int exists(const redis_key& key);
    item* fetch_raw(const redis_key& key);
    int replace(const redis_key& key, item* val);
    int remove(const redis_key& key);
    size_t size();
    item_ptr fetch(const redis_key& key);
    item_ptr random_fetch_and_remove() { return nullptr; }
    std::vector<item_ptr> fetch();
    std::vector<item_ptr> fetch(const std::unordered_set<sstring>& keys);
};

} // namespace redis
