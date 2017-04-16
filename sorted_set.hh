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
#include "core/shared_ptr.hh"
#include "core/sharded.hh"
#include "common.hh"
namespace redis {
using item_ptr = foreign_ptr<lw_shared_ptr<item>>;
class item;
class sorted_set_iterator;
/*
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
    std::vector<foreign_ptr<lw_shared_ptr<item>>> fetch(const std::vector<sstring>& keys);
    int replace(const redis_key& key, lw_shared_ptr<item> item);
    size_t size();
    size_t rank(const redis_key& key, bool reverse);
    int remove(const redis_key& key);
    size_t count(double min, double max);
    size_t remove_range_by_score(double min, double max);
    size_t remove_range_by_rank(long begin, long end);
    std::vector<item_ptr> range_by_rank(long begin, long end, bool reverse);
    std::vector<item_ptr> range_by_score(double min, double max, bool reverse);
    using pred = std::function<bool (lw_shared_ptr<item> m)>;
    size_t range_by_score_if(double min, double max, size_t count, pred&& p);
};
*/
}
