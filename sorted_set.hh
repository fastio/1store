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
#include "core/shared_ptr.hh"
#include "core/sharded.hh"
#include "base.hh"
namespace redis {
using item_ptr = foreign_ptr<lw_shared_ptr<item>>;
class item;
class sorted_set_iterator;
class sorted_set {
private:
    friend class sorted_set_iterator;
    struct rep;
    rep* _rep;
public:
    sorted_set();
    ~sorted_set();
    int exists(const redis_key& key);
    int insert(const redis_key& key, lw_shared_ptr<item> item);
    lw_shared_ptr<item> fetch(const redis_key& key);
    int replace(const redis_key& key, lw_shared_ptr<item> item);
    size_t size();
    size_t rank(const redis_key& key, bool reverse);
    int remove(const redis_key& key);
    size_t count(double min, double max);
    std::vector<item_ptr> range_by_rank(size_t begin, size_t end, bool reverse);
    std::vector<item_ptr> range_by_score(double min, double max, bool reverse);
};
}
