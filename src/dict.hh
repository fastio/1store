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
 *  Copyright (c) 2006-2010, Peng Jian, postack@163.com. All rights reserved.
 *
 */

#ifndef DICT_HH_
#define DICT_HH_
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
    int set(const sstring& key, size_t kh, item* val);
    int exists(const sstring& key, size_t kh);
    item_ptr fetch(const sstring& key, size_t kh);
    item* fetch_raw(const sstring& key, size_t kh);
    int replace(const sstring& key, size_t kh, item* val);
    int remove(const sstring& key, size_t kh);
};

} // namespace redis
#endif /* __DICT_H */
