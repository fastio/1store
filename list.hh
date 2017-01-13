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
#include <vector>
namespace redis {
class item;
using item_ptr = foreign_ptr<boost::intrusive_ptr<item>>;

class list_iterator;

class list : public object {
private:
    friend class list_iterator;
    struct rep;
    rep* _rep;
public:
    list();
    virtual ~list();
    int add_head(item* val);
    int add_tail(item* val);
    item_ptr pop_head();
    item_ptr pop_tail();
    int insert_before(const sstring& pivot, item *value);
    int insert_after(const sstring& pivot, item *value);
    int set(long idx, item *value);
    void remove(const sstring& target);
    item_ptr index(long index);
    std::vector<item_ptr> range(int start, int end);
    int trim(int start, int end);
    int trem(int count, sstring& value);
    long length();
};
}
