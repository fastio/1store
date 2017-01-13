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
#include "db.hh"
#include "redis_commands.hh"
namespace redis {

static constexpr double default_slab_growth_factor = 1.25;
static constexpr uint64_t default_slab_page_size = 1UL*MB;
static constexpr uint64_t default_per_cpu_slab_size = 0UL; // zero means reclaimer is enabled.
static const sstring LIST { "list" };
static const sstring DICT { "dict" };
static const sstring MISC { "misc" };
static const sstring SET  { "set"  };

__thread slab_allocator<item>* _slab;
__thread redis_commands* _redis_commands_ptr;

db::db() : _store(new dict())
         , _misc_storage(MISC, _store)
         , _list_storage(LIST, _store)
         , _dict_storage(DICT, _store)
         , _set_storage(SET, _store)
{
    _slab = new slab_allocator<item>(default_slab_growth_factor,
            default_per_cpu_slab_size, default_slab_page_size,
            [this](item& item_ref) { intrusive_ptr_release(&item_ref); });
    _redis_commands_ptr = new redis_commands();
}

db::~db()
{
    if (_store != nullptr) {
        delete _store;
    }
    if (_redis_commands_ptr) {
        delete _redis_commands_ptr;
    }
}

}
