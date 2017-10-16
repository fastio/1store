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
#include "core/shared_ptr.hh"
#include "core/future.hh"
#include "core/shared_ptr.hh"
#include "core/sharded.hh"
#include "core/temporary_buffer.hh"
#include "core/metrics_registration.hh"
#include <sstream>
#include <iostream>
#include "structures/geo.hh"
#include "structures/bits_operation.hh"
#include <tuple>
#include "cache.hh"
#include "reply_builder.hh"
#include  <experimental/vector>
#include "config.hh"
#include "utils/bytes.hh"
#include "store/column_family.hh"
#include "store/commit_log.hh"
#include "keys.hh"
#include "store/options.hh"
namespace stdx = std::experimental;
namespace redis {
using namespace store;
using scattered_message_ptr = foreign_ptr<lw_shared_ptr<scattered_message<char>>>;
class sset_lsa;
class database;
extern distributed<database> _the_database;
inline distributed<database>& get_database() {
    return _the_database;
}
inline database& get_local_database() {
    return _the_database.local();
}

enum {
    FLAG_SET_NO = 1 << 0,
    FLAG_SET_EX = 1 << 1,
    FLAG_SET_PX = 1 << 2,
    FLAG_SET_NX = 1 << 3,
    FLAG_SET_XX = 1 << 4,
};

class database final : private logalloc::region {
public:
    database();
    ~database();

    future<scattered_message_ptr> set(const redis_key& rk, bytes& val, long expire, uint32_t flag);

    future<scattered_message_ptr> del(const redis_key& key);


    future<scattered_message_ptr> get(const redis_key& key);

    future<> start();
    future<> stop();

    const redis::config& get_config() const {
        return *_config;
    }
private:
    cache _cache;
    lw_shared_ptr<column_family> _system_store;
    lw_shared_ptr<column_family> _store;
    lw_shared_ptr<commit_log> _commit_log;
    seastar::metrics::metric_groups _metrics;
    write_options _write_opt;
    read_options _read_opt;
    bool _enable_write_disk { false };
    void setup_metrics();
    size_t sum_expiring_entries();
    std::unique_ptr<redis::config> _config;
};
}
