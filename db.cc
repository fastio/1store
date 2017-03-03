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

database::database()
{
    using namespace std::chrono;
    _timer.set_callback([this] { expired_items(); });
    _store = &_data_storages[0];
}

database::~database()
{
}

int database::del(redis_key&& rk)
{
    return _store->remove(rk);
}

int database::exists(redis_key&& rk)
{
    return _store->exists(rk);
}

item_ptr database::get(redis_key&& rk)
{
    return _store->fetch(rk);
}

int database::strlen(redis_key&& rk)
{
    auto i = _store->fetch(rk);
    if (i) {
        return i->value_size();
    }
    return 0;
}
int database::type(redis_key&& rk)
{
    auto it = _store->fetch_raw(rk);
    if (!it) {
        return REDIS_ERR;
    }
    return static_cast<int>(it->type());
}

}
