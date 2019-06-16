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
*  Copyright (c) 2016-2026, Peng Jian, pengjian.uestc@gmail.com. All rights reserved.
*
*/
#pragma once
#include <iomanip>
#include <sstream>
#include <functional>
#include <unordered_map>
#include <vector>
#include "bytes.hh"
#include "bytes_ostream.hh"
#include  <experimental/vector>
#include "seastar/core/sharded.hh"
#include "seastar/core/shared_ptr.hh"
#include "seastar/core/scattered_message.hh"
#include "schema.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/filtered.hpp>

template<typename ContainerType>
struct prefetched_struct {
    const schema_ptr _schema;
    bool _inited = false;
    bool _has_more = false;
    size_t _origin_size = 0;
    ContainerType _data;
    prefetched_struct(const schema_ptr schema) : _schema(schema) {}
    bool has_data() const { return _inited; }
    bool has_more() const { return _has_more; }
    void set_has_more(bool v) { _has_more = v; }
    size_t data_size() const { return _data.size(); }
    size_t origin_size() const { return _origin_size; }
    ContainerType& data() { return _data; }
    const ContainerType& data() const { return _data; }
};

using prefetched_map_type = prefetched_struct<std::vector<std::pair<std::optional<bytes>, std::optional<bytes>>>>;
using map_return_type = lw_shared_ptr<prefetched_map_type>;
using prefetched_zset_type = prefetched_struct<std::vector<std::pair<std::optional<bytes>, std::optional<double>>>>;
using zset_return_type = lw_shared_ptr<prefetched_zset_type>;
using prefetched_bytes = prefetched_struct<bytes>;
using bytes_return_type = lw_shared_ptr<prefetched_bytes>;
using prefetched_mbytes = prefetched_struct<std::vector<std::pair<bytes, bytes>>>;
using mbytes_return_type = lw_shared_ptr<prefetched_mbytes>;

namespace redis {
static const size_t redis_cluser_slots { 16384 };
class redis_message final {
private:
    seastar::lw_shared_ptr<scattered_message<char>> _message;
    /*
    redis_message(bytes_view b) noexcept : redis_message() {
        _message->write(b);
    }
    */
public:
    redis_message() = delete;
    redis_message(const redis_message&) = delete;
    redis_message& operator=(const redis_message&) = delete;
    redis_message(redis_message&& o) noexcept : _message(std::move(o._message)) {}
    redis_message(lw_shared_ptr<scattered_message<char>> m) noexcept : _message(m) {}
    redis_message& operator=(redis_message&& o) noexcept {
        if (this != &o) {
            _message = std::move(o._message);
        }
        return *this;
    }
    static future<redis_message> make_slots(lw_shared_ptr<std::vector<std::tuple<size_t, size_t, bytes, uint16_t>>> peer_slots) {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append(sstring(sprint("*%d\r\n", peer_slots->size())));
        if (peer_slots->size() > 0) {
            for (auto& peer_slot : *peer_slots) {
                m->append(sstring("*3\r\n"));
                m->append(sstring(sprint(":%zd\r\n", std::get<0>(peer_slot))));
                m->append(sstring(sprint(":%zd\r\n", std::get<1>(peer_slot))));
                m->append(sstring(sprint("*2\r\n")));
                auto& p = std::get<2>(peer_slot);
                write_bytes(m, p);
                m->append(sstring(sprint(":%d\r\n", std::get<3>(peer_slot))));
            }
        }
        m->on_delete([ r = std::move(foreign_ptr { peer_slots }) ] {});
        return make_ready_future<redis_message>(m);
    }
    static future<redis_message> make_zset_bytes(lw_shared_ptr<std::vector<std::optional<bytes>>> r);
    static future<redis_message> make_exception(sstring data) {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append(data);
        return make_ready_future<redis_message>(m);
    }
    static future<redis_message> make_long(const long content) {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append(sstring(sprint(":%lld\r\n", content)));
        return make_ready_future<redis_message>(m);
    }
    static future<redis_message> make_empty_list_bytes() {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append_static("*0\r\n");
        return make_ready_future<redis_message>(m);
    }
    static future<redis_message> make_list_bytes(map_return_type r, size_t begin, size_t end);
    static future<redis_message> make_list_bytes(map_return_type r, size_t index);
    static future<redis_message> make_bytes(bytes_return_type r) {
        auto m = make_lw_shared<scattered_message<char>> ();
        assert(r->has_data());
        write_bytes(m, r->data());
        m->on_delete([ r = std::move(foreign_ptr { r }) ] {});
        return make_ready_future<redis_message>(m);
    }
    static future<redis_message> make_bytes(const bytes& b) {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append(sstring(sprint("$%d\r\n", b.size())));
        m->append(sstring(reinterpret_cast<const char*>(b.data()), b.size()));
        m->append_static("\r\n");
        return make_ready_future<redis_message>(m);
    }
    static future<redis_message> make_map_key_bytes(map_return_type r);
    static future<redis_message> make_map_val_bytes(map_return_type r);
    static future<redis_message> make_map_bytes(map_return_type r);
    static future<redis_message> make_set_bytes(map_return_type r, size_t index);
    static future<redis_message> make_set_bytes(map_return_type r, std::vector<size_t> index);
    static future<redis_message> make_mbytes(mbytes_return_type r);
    static future<redis_message> one() {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append_static(":1\r\n");
        return make_ready_future<redis_message>(m);
    }
    static future<redis_message> zero() {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append_static(":0\r\n");
        return make_ready_future<redis_message>(m);
    }
    static future<redis_message> ok() {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append_static("+OK\r\n");
        return make_ready_future<redis_message>(m);
    }
    static future<redis_message> err() {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append_static(":0\r\n");
        return make_ready_future<redis_message>(m);
    }
    static future<redis_message> null() {
        auto m = make_lw_shared<scattered_message<char>> ();
        m->append_static("$-1\r\n");
        return make_ready_future<redis_message>(m);
    }
    inline lw_shared_ptr<scattered_message<char>> message() { return _message; }
private:
    static void write_bytes(lw_shared_ptr<scattered_message<char>> m, bytes& b) {
        m->append(sstring(sprint("$%d\r\n", b.size())));
        m->append_static(reinterpret_cast<const char*>(b.data()), b.size());
        m->append_static("\r\n");
    }
};
}
