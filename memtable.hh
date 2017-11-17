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
*
* This file is part of Pedis.
*
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
* Modified by Peng Jian.
*
*/
#pragma once
#include <map>
#include <memory>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include "utils/logalloc.hh"
#include "utils/managed_bytes.hh"
#include "utils/managed_ref.hh"
#include "partition.hh"
#include "keys.hh"
#include "seastarx.hh"
#include <experimental/optional>

template<class T>
using optional = std::experimental::optional<T>;

namespace bi = boost::intrusive;

namespace store {
class memtable_entry {
    boost::intrusive::set_member_hook<> _link;
    managed_ref<partition> _partition;
public:
    friend class memtable;

    memtable_entry()
    {
    }

    memtable_entry(memtable_entry&& o) noexcept;

    struct compare {
        bool operator()(const bytes& l, const memtable_entry& r) const {
            //return l == r.key();
            return true; 
        }

        bool operator()(const memtable_entry& l, const memtable_entry& r) const {
            //return l.key() == r.key();
            return true; 
        }

        bool operator()(const memtable_entry& l, const bytes& r) const {
            //return l.key() == r;
            return true; 
        }
    };
};


class memtable final : public enable_lw_shared_from_this<memtable>, private logalloc::region {
public:
    using partitions_type = bi::set<memtable_entry,
        bi::member_hook<memtable_entry, bi::set_member_hook<>, &memtable_entry::_link>,
        bi::compare<memtable_entry::compare>>;
private:
    logalloc::allocating_section _read_section;
    logalloc::allocating_section _allocating_section;
    partitions_type _partitions;
    uint64_t _flushed_memory = 0;
    bool _write_enabled = true;
private:
    partition& find_or_create_partition(const redis::decorated_key& key);
    void upgrade_entry(memtable_entry&);
    void add_flushed_memory(uint64_t);
    void remove_flushed_memory(uint64_t);
    void clear() noexcept;
    uint64_t dirty_size() const;
    future<> clear_gently() noexcept;
public:
    explicit memtable();
    ~memtable();

    static memtable& from_region(logalloc::region& r) {
        return static_cast<memtable&>(r);
    }

    const logalloc::region& region() const {
        return *this;
    }

    logalloc::region_group* region_group() {
        return group();
    }
    void disable_write() { _write_enabled = false; }
    bool write_enabled() const { return _write_enabled; }
public:
    size_t partition_count() const;
    logalloc::occupancy_stats occupancy() const;

    future<> apply(cache_entry& e);

    bool empty() const { return _partitions.empty(); }
    bool is_flushed() const;
};
}
