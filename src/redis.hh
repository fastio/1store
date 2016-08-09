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
 *
 *  Copyright (c) 2006-2010, Peng Jian, pstack@163.com. All rights reserved.
 *
 */
#pragma once
#include <functional>
#include "core/sharded.hh"
#include "core/sstring.hh"
#include <experimental/optional>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <iomanip>
#include <sstream>
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/timer-set.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/units.hh"
#include "core/distributed.hh"
#include "core/vector-data-sink.hh"
#include "core/bitops.hh"
#include "core/slab.hh"
#include "core/align.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include <unistd.h>
#include <cstdlib>
#include "base.hh"
namespace redis {

namespace stdx = std::experimental;

struct args_collection;
class db;
class item;
using item_ptr = foreign_ptr<boost::intrusive_ptr<item>>;
class sharded_redis {
private:
    distributed<db>& _peers;
    inline unsigned get_cpu(const size_t h) {
        return h % smp::count;
    }
public:
    sharded_redis(distributed<db>& peers) : _peers(peers) {}

    // [TEST APIs]
    future<sstring> echo(args_collection& args);
    // [COUNTER APIs]
    future<uint64_t> incr(args_collection& args);
    future<uint64_t> decr(args_collection& args);
    future<uint64_t> incrby(args_collection& args);
    future<uint64_t> decrby(args_collection& args);

    // [STRING APIs]
    future<int> set(args_collection& args);
    future<int> del(args_collection& args);
    future<int> exists(args_collection& args);
    future<int> append(args_collection& args);
    future<int> strlen(args_collection& args);
    future<item_ptr> get(args_collection& args);

    // [LIST APIs]
    future<int> lpush(args_collection& arg);
    future<int> lpushx(args_collection& args);
    future<int> rpush(args_collection& arg);
    future<int> rpushx(args_collection& args);
    future<item_ptr> lpop(args_collection& args);
    future<item_ptr> rpop(args_collection& args);
    future<int> llen(args_collection& args);
    future<item_ptr> lindex(args_collection& args);
    future<int> linsert(args_collection& args);
    future<int> lset(args_collection& args);
    future<std::vector<item_ptr>> lrange(args_collection& args);
    future<int> ltrim(args_collection& args);
    future<int> lrem(args_collection& args);
private:
    future<item_ptr> pop_impl(args_collection& args, bool left);
    future<int> push_impl(args_collection& arg, bool force, bool left);
    future<int> push_impl(sstring& key, sstring& value, bool force, bool left);
    future<int> remove_impl(const sstring& key, size_t hash);
    future<uint64_t> counter_by(args_collection& args, bool incr, bool with_step);
};

} /* namespace redis */
