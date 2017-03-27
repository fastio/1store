/*
* Pedis is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* You may obtain a copy of the License at
*
*     http://www.gnu.org/licenses
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
#include "common.hh"
namespace redis {
class item;
class dict_iterator;
class dict {
private:
    friend class dict_iterator;
    struct rep;
    rep* _rep;
public:
    dict();
    ~dict();
    int set(const redis_key& key, lw_shared_ptr<item> val);
    int exists(const redis_key& key);
    int exists(lw_shared_ptr<item> key);
    lw_shared_ptr<item> fetch_raw(const redis_key& key);
    int replace(const redis_key& key, lw_shared_ptr<item> val);
    int remove(const redis_key& key);
    int remove(lw_shared_ptr<item> item);
    size_t size();
    foreign_ptr<lw_shared_ptr<item>> fetch(const redis_key& key);
    foreign_ptr<lw_shared_ptr<item>> random_fetch_and_remove() { return nullptr; }
    std::vector<foreign_ptr<lw_shared_ptr<item>>> fetch();
    std::vector<foreign_ptr<lw_shared_ptr<item>>> fetch(const std::vector<sstring>& keys);
};

} // namespace redis
