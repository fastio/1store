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
namespace redis {

static const sstring LIST { "list" };
static const sstring DICT { "dict" };
static const sstring MISC { "misc" };
static const sstring SET  { "set"  };


db::db() : _store(new dict())
         , _misc_storage(MISC, _store)
         , _list_storage(LIST, _store)
         , _dict_storage(DICT, _store)
         , _set_storage(SET, _store)
{
    using namespace std::chrono;
    _timer.set_callback([this] { expired_items(); });
}

db::~db()
{
    if (_store != nullptr) {
        delete _store;
    }
}

}
