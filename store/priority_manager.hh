/*
* Copyright 2016 ScyllaDB
*
* Scylla is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* Scylla is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
*/

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
*  Modified by Peng Jian, pengjian.uestc@gmail.com.
*
*/
#pragma once
#include <core/future.hh>
#include <core/distributed.hh>
#include <core/reactor.hh>

#include "seastarx.hh"

namespace store {
class priority_manager {
    ::io_priority_class _commitlog_priority;
    ::io_priority_class _mt_flush_priority;
    ::io_priority_class _write_priority;
    ::io_priority_class _read_priority;
    ::io_priority_class _compaction_priority;

public:
    const ::io_priority_class&
    commitlog_priority() {
        return _commitlog_priority;
    }

    const ::io_priority_class&
    memtable_flush_priority() {
        return _mt_flush_priority;
    }

    const ::io_priority_class&
    read_priority() {
        return _read_priority;
    }

    const ::io_priority_class&
    write_priority() {
        return _write_priority;
    }

    const ::io_priority_class&
    compaction_priority() {
        return _compaction_priority;
    }

    priority_manager()
        : _commitlog_priority(engine().register_one_priority_class("commitlog", 100))
        , _mt_flush_priority(engine().register_one_priority_class("memtable_flush", 100))
        , _write_priority(engine().register_one_priority_class("write", 20))
        , _read_priority(engine().register_one_priority_class("read", 20))
        , _compaction_priority(engine().register_one_priority_class("compaction", 100))

    {}
};

priority_manager& get_local_priority_manager();
const inline ::io_priority_class&
get_local_commitlog_priority() {
    return get_local_priority_manager().commitlog_priority();
}

const inline ::io_priority_class&
get_local_memtable_flush_priority() {
    return get_local_priority_manager().memtable_flush_priority();
}

const inline ::io_priority_class&
get_local_read_priority() {
    return get_local_priority_manager().read_priority();
}

const inline ::io_priority_class&
get_local_write_priority() {
    return get_local_priority_manager().write_priority();
}

const inline ::io_priority_class&
get_local_compaction_priority() {
    return get_local_priority_manager().compaction_priority();
}
}
