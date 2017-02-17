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
#include <vector>
namespace redis {
class item;
class list_iterator;

class list {
private:
    friend class list_iterator;
    struct rep;
    rep* _rep;
public:
    list();
    ~list();
    int add_head(lw_shared_ptr<item> val);
    int add_tail(lw_shared_ptr<item> val);
    foreign_ptr<lw_shared_ptr<item>> pop_head();
    foreign_ptr<lw_shared_ptr<item>> pop_tail();
    int insert_before(const sstring& pivot, lw_shared_ptr<item> value);
    int insert_after(const sstring& pivot, lw_shared_ptr<item> value);
    int set(long idx, lw_shared_ptr<item> value);
    void remove(const sstring& target);
    foreign_ptr<lw_shared_ptr<item>> index(long index);
    std::vector<foreign_ptr<lw_shared_ptr<item>>> range(int start, int end);
    int trim(int start, int end);
    int trem(int count, sstring& value);
    long length();
};
}
