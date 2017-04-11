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
#include <boost/intrusive/list.hpp>
#include "utils/allocation_strategy.hh"
#include "utils/managed_ref.hh"
#include "utils/managed_bytes.hh"
namespace redis {
class list_lsa {
    struct internal_node {
        boost::intrusive::list_member_hook<> _link;
        managed_bytes _data;
        internal_node(bytes_view data) noexcept : _link(), _data(data) {}
        internal_node(internal_node&& o) noexcept
            : _link(std::move(o._link)), _data(std::move(o._data))
        {
        }
        inline bool operator==(const internal_node& o) const
        {
            return _data == o._data;
        }
    };
    using internal_list_type = boost::intrusive::list<internal_node,
                                                      boost::intrusive::member_hook<internal_node, boost::intrusive::list_member_hook<>,
                                                      &internal_node::_link>>;
    using const_iterator = internal_list_type::const_iterator;
    using iterator = internal_list_type::iterator;
    internal_list_type  _list;
public:
    list_lsa() noexcept
    {
    }

    list_lsa(list_lsa&& o) noexcept : _list(std::move(o._list))
    {
    }

    ~list_lsa()
    {
        clear();
    }
    // Inserts the value in the front of the list.
    inline void insert_head(const sstring& data)
    {
        bytes_view v {reinterpret_cast<const signed char*>(data.data()), data.size()};
        auto entry = current_allocator().construct<internal_node>(v);
        _list.push_front(*entry);
    }

    // Inserts the value in the back of the list.
    inline void insert_tail(const sstring& data)
    {
        bytes_view v {reinterpret_cast<const signed char*>(data.data()), data.size()};
        auto entry = current_allocator().construct<internal_node>(v);
        _list.push_back(*entry);
    }

    inline void insert_at(size_t index, const sstring& data)
    {
        bytes_view v {reinterpret_cast<const signed char*>(data.data()), data.size()};
        auto entry = current_allocator().construct<internal_node>(v);
        auto p = _list.begin();
        for (size_t i = 0; i < index && p != _list.end(); ++i, ++p) {}
        _list.insert(p, *entry);
    }

    inline const managed_bytes& at(long index) const
    {
        while (index < 0) index += static_cast<long>(_list.size());
        while (index > static_cast<long>(_list.size())) index -= static_cast<long>(_list.size()); 
        auto p = _list.begin();
        for (size_t i = 0; i < static_cast<size_t>(index) && p != _list.end(); ++i, ++p) {}
        return p->_data;
    }

    inline managed_bytes& at(long index)
    {
        while (index < 0) index += static_cast<long>(_list.size());
        while (index > static_cast<long>(_list.size())) index -= static_cast<long>(_list.size()); 
        auto p = _list.begin();
        for (size_t i = 0; i < static_cast<size_t>(index) && p != _list.end(); ++i, ++p) {}
        return p->_data;
    }

    // Returns a reference to the data of first element of the list.
    inline const managed_bytes& front() const
    {
        auto& front_ref = _list.front();
        return front_ref._data;
    }

    // Erases the first element of the list. Destructors are called.
    inline void pop_front()
    {
        _list.pop_front_and_dispose(current_deleter<internal_node>());
    }

    // Returns a reference to the data of last element of the list.
    inline const managed_bytes& back() const
    {
        auto& back_ref = _list.back();
        return back_ref._data;
    }

    // Erases the last element of the list. Destructors are called.
    inline void pop_back()
    {
        _list.pop_back_and_dispose(current_deleter<internal_node>());
    }

    inline bool empty() const
    {
        return _list.empty();
    }

    // Returns the number of the elements contained in the list.
    inline size_t size() const
    {
        return static_cast<size_t>(_list.size());
    }

    // Erase the elements from the list.
    inline void erase(const sstring& data)
    {
        trem(data, 1);
    }

    inline size_t trem(const std::string& data, size_t count)
    {
        size_t erased = 0;
        for (auto it = _list.begin(); it != _list.end(); ++it) {
            if (equal(*it, data)) {
                ++erased;
                const_iterator eit = it;
                ++it;
                _list.erase_and_dispose(eit, current_deleter<internal_node>());
                if (erased == count) {
                    break;
                }
            }
        }
        return erased;
    }
    inline bool trim(long start, long end)
    {
        long lr = 0, rr = 0;
        if (start < 0) start += static_cast<long>(_list.size());
        if (end < 0) end += static_cast<long>(_list.size());

        if (end < 0)
            return false;

        if (start < 0) start = 0;
        if (start > end || start >= static_cast<int>(_list.size())) {
            lr = _list.size(); // all nodes were removed
            rr = 0;
        }
        else {
            if (end > static_cast<long>(_list.size())) end = _list.size() - 1;
            lr = start;
            rr = static_cast<long>(_list.size()) - end - 1;
        }
        remove_by_range(0, lr);
        remove_by_range(-rr, rr);
        return true;
    }
    // Erases all the elements of the list. Destructors are called.
    inline void clear()
    {
        _list.clear_and_dispose(current_deleter<internal_node>());
    }

    // reduce
    void reduce(size_t start, size_t end, std::function<void(const_iterator it)>&& reduce_fn)
    {
        auto start_it = iterator_to(start);
        auto end_it = iterator_to(end);
        for (; start_it != end_it; ++start_it) {
            reduce_fn(start_it);
        }
    }
private:
    void remove_by_range(long start, long count)
    {
        if (count > 0) {
            while (start < 0) start += static_cast<long>(_list.size());
            size_t eidx = static_cast<size_t>(start + count);
            if (eidx > _list.size()) eidx = _list.size() - 1;
            auto s = iterator_to(static_cast<size_t>(start));
            auto e = iterator_to(eidx);
            _list.erase_and_dispose(s, e, current_deleter<internal_node>());
        }
    }
    const_iterator iterator_to(size_t index) const
    {
        auto i = _list.begin();
        for (size_t idx = 0; idx < index && i != _list.end(); ++idx, ++i) {};
        return i;
    }
    iterator iterator_to(size_t index)
    {
        auto i = _list.begin();
        for (size_t idx = 0; idx < index && i != _list.end(); ++idx, ++i) {};
        return i;
    }

    inline bool equal(const internal_node& n, const sstring& data) const
    {
        auto mb = n._data;
        bytes_view b {mb.data(), mb.size()} ;
        bytes_view a {reinterpret_cast<const signed char*>(data.data()), data.size()};
        return b == a;
    }

    inline bool equal(const sstring& data, const internal_node& n) const
    {
        return equal(data, n);
    }

};
}
