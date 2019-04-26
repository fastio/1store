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
namespace redis {
static const bytes msg_crlf {"\r\n"};
static const bytes msg_ok {"+OK\r\n"};
static const bytes msg_pong {"+PONG\r\n"};
static const bytes msg_err = {"-ERR\r\n"};
static const bytes msg_zero = {":0\r\n"};
static const bytes msg_one = {":1\r\n"};
static const bytes msg_neg_one = {":-1\r\n"};
static const bytes msg_neg_two = {":-2\r\n"};
static const bytes msg_null_blik = {"$-1\r\n"};
static const bytes msg_null_multi_bulk = {"*-1\r\n"};
static const bytes msg_empty_multi_bulk = {"*0\r\n"};
static const bytes msg_type_err = {"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"};
static const bytes msg_nokey_err = {"-ERR no such key\r\n"};
static const bytes msg_nocmd_err = {"-ERR no such command\r\n"};
static const bytes msg_syntax_err = {"-ERR syntax error\r\n"};
static const bytes msg_same_object_err = {"-ERR source and destination objects are the same\r\n"};
static const bytes msg_out_of_range_err = {"-ERR index out of range\r\n"};
static const bytes msg_not_integer_err = {"-ERR ERR hash value is not an integer\r\n" };
static const bytes msg_not_float_err = {"-ERR ERR hash value is not an float\r\n" };
static const bytes msg_str_tag = {"+"};
static const bytes msg_num_tag = {":"};
static const bytes msg_sigle_tag = {"*"};
static const bytes msg_batch_tag = {"$"};
static const bytes msg_not_found = {"+(nil)\r\n"};
static const bytes msg_nil = {"+(nil)\r\n"};
static constexpr const int REDIS_OK = 0;
static constexpr const int REDIS_ERR = 1;
static constexpr const int REDIS_NONE = -1;
static constexpr const int REDIS_WRONG_TYPE = -2;
static const bytes msg_type_string {"+string\r\n"};
static const bytes msg_type_none {"+none\r\n"};
static const bytes msg_type_list {"+list\r\n"};
static const bytes msg_type_hll {"+hyperloglog\r\n"};
static const bytes msg_type_set {"+set\r\n"};
static const bytes msg_type_zset {"+zset\r\n"};
static const bytes msg_type_hash {"+hash\r\n"};
static const size_t redis_cluser_slots { 16384 };
class redis_message final {
private:
    std::unique_ptr<bytes_ostream> _message;
    redis_message() noexcept : _message(std::make_unique<bytes_ostream>()) {}
    redis_message(bytes_view b) noexcept : redis_message() {
        _message->write(b);
    }
public:
    redis_message(const redis_message&) = delete;
    redis_message& operator=(const redis_message&) = delete;
    redis_message(redis_message&& o) noexcept : _message(std::move(o._message)) {}
    redis_message& operator=(redis_message&& o) noexcept {
        if (this != &o) {
            _message = std::move(o._message);
        }
        return *this;
    }
   
    static future<redis_message> make(std::vector<std::tuple<size_t, size_t, bytes, uint16_t>>&& peer_slots) {
        redis_message m;
        m.write_sstring(sstring(sprint("*%d\r\n", peer_slots.size())));
        if (peer_slots.size() > 0) {
            for (auto& peer_slot : peer_slots) {
                m.write_sstring(sstring("*3\r\n"));
                m.write_sstring(sstring(sprint(":%zd\r\n", std::get<0>(peer_slot))));
                m.write_sstring(sstring(sprint(":%zd\r\n", std::get<1>(peer_slot))));
                m.write_sstring(sstring(sprint("*2\r\n")));
                m.write_sstring(sstring(sprint("$%d\r\n", std::get<2>(peer_slot).size())));
                m.write(bytes_view(std::get<2>(peer_slot)));
                m.write(bytes_view(msg_crlf));
                m.write_sstring(sstring(sprint(":%d\r\n", std::get<3>(peer_slot))));
            }
        }
        return make_ready_future<redis_message>(std::move(m));
    }
    bytes_ostream& ostream() { return *_message; }
    static future<redis_message> make(const bytes& content) {
        redis_message m;
        m.write_sstring(sstring(sprint("$%d\r\n", content.size())));
        m.write(bytes_view(content));
        m.write(bytes_view(msg_crlf));
        return make_ready_future<redis_message>(std::move(m));
    }
    static future<redis_message> make_exception(bytes&& content) {
        redis_message m;
        m.write(bytes_view(content));
        return make_ready_future<redis_message>(std::move(m));
    }
    static future<redis_message> make(bytes&& content) {
        redis_message m;
        m.write_sstring(sstring(sprint("$%d\r\n", content.size())));
        m.write(bytes_view(content));
        m.write(bytes_view(msg_crlf));
        return make_ready_future<redis_message>(std::move(m));
    }
    static future<redis_message> make(const size_t content) {
        redis_message m;
        m.write_sstring(sstring(sprint(":%zu\r\n", content)));
        return make_ready_future<redis_message>(std::move(m));
    }
    static future<redis_message> make(const long content) {
        redis_message m;
        m.write_sstring(sstring(sprint(":%lld\r\n", content)));
        return make_ready_future<redis_message>(std::move(m));
    }
    static future<redis_message> make(std::vector<bytes>&& content) {
        redis_message m;
        m.write_sstring(sstring(sprint("*%zu\r\n", content.size())));
        for (size_t i = 0; i < content.size(); ++i) {
            m.write_sstring(sstring(sprint("$%zu\r\n", content[i].size())));
            m.write(bytes_view(content[i]));
            m.write(bytes_view(msg_crlf));
        }
        return make_ready_future<redis_message>(std::move(m));
    }
    static future<redis_message> make(std::vector<std::optional<bytes>>&& content) {
        redis_message m;
        m.write_sstring(sstring(sprint("*%zu\r\n", content.size())));
        for (size_t i = 0; i < content.size(); ++i) {
            auto& s = *(content[i]);
            m.write_sstring(sstring(sprint("$%zu\r\n", s.size())));
            m.write(bytes_view(s));
            m.write(bytes_view(msg_crlf));
        }
        return make_ready_future<redis_message>(std::move(m));
    }
    static future<redis_message> one() {
        redis_message m;
        m.write(bytes_view(msg_one));
        return make_ready_future<redis_message>(std::move(m));
    }
    static future<redis_message> zero() {
        redis_message m;
        m.write(bytes_view(msg_zero));
        return make_ready_future<redis_message>(std::move(m));
    }
    static future<redis_message> ok() {
        redis_message m;
        m.write(bytes_view(msg_ok));
        return make_ready_future<redis_message>(std::move(m));
    }
    static future<redis_message> err() {
        redis_message m;
        m.write(bytes_view(msg_zero));
        return make_ready_future<redis_message>(std::move(m));
    }
    static future<redis_message> null() {
        redis_message m;
        m.write(bytes_view(msg_null_blik));
        return make_ready_future<redis_message>(std::move(m));
    }
private:
    inline void write(const bytes_view content) {
        _message->write(content);
    }
    inline void write_sstring(sstring&& content) {
        _message->write(bytes_view(reinterpret_cast<const int8_t*>(content.data()), content.size()));
    }
};
}
