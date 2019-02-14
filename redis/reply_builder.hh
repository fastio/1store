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
#include "seastar/core/shared_ptr.hh"
#include "seastar/core/sharded.hh"
#include "seastar/core/future.hh"
#include "seastar/core/sstring.hh"
#include "bytes.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
using namespace seastar;
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

class ok_tag {};
class error_tag {};
class error_message_tag {};
class null_message_tag{};
class message_tag{};

class reply_builder final {
public:
static sstring to_sstring(bytes b) {
   return sstring{reinterpret_cast<char*>(b.data()), b.size()};
}

template<typename tag>
static future<reply> build()
{
    auto m = make_lw_shared<scattered_message<char>>();
    if (std::is_same<ok_tag, tag>::value) {
        m->append(to_sstring(msg_one));
    }
    else if (std::is_same<null_message_tag, tag>::value) {
        m->append(to_sstring(msg_null_blik));
    } else {
        m->append(to_sstring(msg_zero));
    }
    return make_ready_future<reply>(reply { foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m) });
}

template<typename tag>
static future<reply> build(bytes&& data) {
    auto m = make_lw_shared<scattered_message<char>>();
    if (std::is_same<message_tag, tag>::value) {
        m->append(to_sstring(msg_batch_tag));
        m->append(sstring(sprint("%d\r\n", data.size())));
        m->append(to_sstring(data));
        m->append_static(to_sstring(msg_crlf));
    }
    else if (std::is_same<error_message_tag, tag>::value) {
        m->append(to_sstring(data));
    }
    return make_ready_future<reply>(reply { foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m) });
}

/*
template<bool Key, bool Value>
static future<reply> build(const cache_entry* e)
{
    if (e) {
        //build reply
        auto m = make_lw_shared<scattered_message<char>>();
        if (Key) {
            m->append(msg_batch_tag);
            m->append(to_sstring(e->key_size()));
            m->append_static(msg_crlf);
            m->append(sstring{e->key_data(), e->key_size()});
            m->append_static(msg_crlf);
        }
        if (Value) {
            m->append_static(msg_batch_tag);
            if (e->type_of_integer()) {
               auto&& n = to_sstring(e->value_integer());
               m->append(to_sstring(n.size()));
               m->append_static(msg_crlf);
               m->append(n);
               m->append_static(msg_crlf);
            }
            else if (e->type_of_float()) {
               auto&& n = to_sstring(e->value_float());
               m->append(to_sstring(n.size()));
               m->append_static(msg_crlf);
               m->append(n);
               m->append_static(msg_crlf);
            }
            else if (e->type_of_bytes()) {
                m->append(to_sstring(e->value_bytes_size()));
                m->append_static(msg_crlf);
                m->append(sstring{e->value_bytes_data(), e->value_bytes_size()});
                m->append_static(msg_crlf);
            }
            else {
               m->append_static(msg_type_err);
            }
            return make_ready_future<scattered_message_ptr>(reply { foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m) });
        }
    }
    else {
        return reply_builder::build(msg_not_found);
    }
}

template<bool Key, bool Value>
static future<reply> build(const std::vector<const dict_entry*>& entries)
{
    if (!entries.empty()) {
        //build reply
        auto m = make_lw_shared<scattered_message<char>>();
        m->append_static(msg_sigle_tag);
        if (Key && Value) {
           m->append(std::move(to_sstring(entries.size() * 2)));
        }
        else {
           m->append(std::move(to_sstring(entries.size())));
        }
        m->append_static(msg_crlf);
        for (size_t i = 0; i < entries.size(); ++i) {
            const auto e = entries[i];
            if (Key) {
                if (e) {
                    m->append_static(msg_batch_tag);
                    m->append(to_sstring(e->key_size()));
                    m->append_static(msg_crlf);
                    m->append(sstring{e->key_data(), e->key_size()});
                    m->append_static(msg_crlf);
                }
                else {
                    m->append_static(msg_not_found);
                }
            }
            if (Value) {
                if (e) {
                    m->append_static(msg_batch_tag);
                    if (e->type_of_integer()) {
                        auto&& n = to_sstring(e->value_integer());
                        m->append(to_sstring(n.size()));
                        m->append_static(msg_crlf);
                        m->append(n);
                        m->append_static(msg_crlf);
                    }
                    else if (e->type_of_float()) {
                        auto&& n = to_sstring(e->value_float());
                        m->append(to_sstring(n.size()));
                        m->append_static(msg_crlf);
                        m->append(n);
                        m->append_static(msg_crlf);
                    }
                    else if (e->type_of_bytes()) {
                        m->append(to_sstring(e->value_bytes_size()));
                        m->append_static(msg_crlf);
                        m->append(sstring{e->value_bytes_data(), e->value_bytes_size()});
                        m->append_static(msg_crlf);
                    }
                    else {
                        m->append_static(msg_type_err);
                    }
                }
                else {
                    m->append_static(msg_not_found);
                }
            }
        }
        return make_ready_future<scattered_message_ptr>(reply { foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m) });
    }
    else {
        return reply_builder::build(msg_nil);
    }
}

template<bool Key, bool Value>
static future<reply> build(const dict_entry* e)
{
    if (e) {
        //build reply
        auto m = make_lw_shared<scattered_message<char>>();
        if (Key) {
            m->append(msg_batch_tag);
            m->append(to_sstring(e->key_size()));
            m->append_static(msg_crlf);
            m->append(sstring{e->key_data(), e->key_size()});
            m->append_static(msg_crlf);
        }
        if (Value) {
            if (e->type_of_integer()) {
               auto&& n = to_sstring(e->value_integer());
               m->append(to_sstring(n.size()));
               m->append_static(msg_crlf);
               m->append(n);
               m->append_static(msg_crlf);
            }
            else if (e->type_of_float()) {
               auto&& n = to_sstring(e->value_float());
               m->append(to_sstring(n.size()));
               m->append_static(msg_crlf);
               m->append(n);
               m->append_static(msg_crlf);
            }
            else if (e->type_of_bytes()) {
                m->append_static(msg_batch_tag);
                m->append(to_sstring(e->value_bytes_size()));
                m->append_static(msg_crlf);
                m->append(sstring{e->value_bytes_data(), e->value_bytes_size()});
                m->append_static(msg_crlf);
            }
            else {
               m->append_static(msg_type_err);
            }
            return make_ready_future<scattered_message_ptr>(reply { foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m) });
        }
        return make_ready_future<scattered_message_ptr>(reply { foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m) });
    }
    else {
        return reply_builder::build(msg_nil);
    }
}

static future<reply> build(const std::vector<const managed_bytes*>& data)
{
    auto m = make_lw_shared<scattered_message<char>>();
    m->append(msg_sigle_tag);
    m->append(to_sstring(data.size()));
    m->append_static(msg_crlf);
    for (size_t i = 0; i < data.size(); ++i) {
        m->append_static(msg_batch_tag);
        m->append(to_sstring(data[i]->size()));
        m->append_static(msg_crlf);
        m->append(sstring{reinterpret_cast<const char*>(data[i]->data()), data[i]->size()});
        m->append_static(msg_crlf);
    }
    return make_ready_future<scattered_message_ptr>(reply { foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m) });
}

static future<reply> build(const managed_bytes& data)
{
    auto m = make_lw_shared<scattered_message<char>>();
    m->append_static(msg_batch_tag);
    m->append(to_sstring(data.size()));
    m->append_static(msg_crlf);
    m->append(sstring{reinterpret_cast<const char*>(data.data()), data.size()});
    m->append_static(msg_crlf);
    return make_ready_future<scattered_message_ptr>(reply { foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m) });
}

static future<reply> build(const std::vector<const sset_entry*>& entries, bool with_score)
{
    if (!entries.empty()) {
        //build reply
        auto m = make_lw_shared<scattered_message<char>>();
        m->append_static(msg_sigle_tag);
        if (with_score) {
            m->append(std::move(to_sstring(entries.size() * 2)));
        }
        else {
            m->append(std::move(to_sstring(entries.size())));
        }
        m->append_static(msg_crlf);
        for (size_t i = 0; i < entries.size(); ++i) {
            const auto& e = entries[i];
            assert(e != nullptr);
            m->append_static(msg_batch_tag);
            m->append(to_sstring(e->key_size()));
            m->append_static(msg_crlf);
            m->append(sstring{e->key_data(), e->key_size()});
            m->append_static(msg_crlf);
            if (with_score) {
                m->append_static(msg_batch_tag);
                auto&& n = to_sstring(e->score());
                m->append(to_sstring(n.size()));
                m->append_static(msg_crlf);
                m->append(n);
                m->append_static(msg_crlf);
            }
        }
        return make_ready_future<scattered_message_ptr>(reply { foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m) });
    }
    else {
        return reply_builder::build(msg_nil);
    }
}

static future<reply> build(std::vector<bytes>& data)
{
    auto m = make_lw_shared<scattered_message<char>>();
    m->append_static(msg_sigle_tag);
    m->append(std::move(to_sstring(data.size())));
    m->append_static(msg_crlf);
    for (size_t i = 0; i < data.size(); ++i) {
        auto& uu = data[i];
        m->append_static(msg_batch_tag);
        m->append(to_sstring(uu.size()));
        m->append_static(msg_crlf);
        m->append(std::move(uu));
        m->append_static(msg_crlf);
    }
    return make_ready_future<scattered_message_ptr>(reply { foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m) });
}

static  future<reply> build(std::vector<foreign_ptr<lw_shared_ptr<bytes>>>& entries)
{
    if (!entries.empty()) {
        auto m = make_lw_shared<scattered_message<char>>();
        m->append_static(msg_sigle_tag);
        m->append(std::move(to_sstring(entries.size())));
        m->append_static(msg_crlf);
        for (size_t i = 0; i < entries.size(); ++i) {
            auto& e = entries[i];
            m->append_static(msg_batch_tag);
            m->append(to_sstring(e->size()));
            m->append_static(msg_crlf);
            m->append(*e);
            m->append_static(msg_crlf);
        }
        return make_ready_future<scattered_message_ptr>(reply { foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m) });
    }
    return reply_builder::build(msg_nil);
}

static future<reply> build(std::unordered_map<sstring, double>& data, bool with_score)
{
    auto m = make_lw_shared<scattered_message<char>>();
    m->append_static(msg_sigle_tag);
    if (with_score) {
        m->append(std::move(to_sstring(data.size() * 2)));
    }
    else {
        m->append(std::move(to_sstring(data.size())));
    }
    m->append_static(msg_crlf);
    for (auto& d : data) {
        m->append_static(msg_batch_tag);
        m->append(to_sstring(d.first.size()));
        m->append_static(msg_crlf);
        m->append(std::move(d.first));
        m->append_static(msg_crlf);
        if (with_score) {
            m->append_static(msg_batch_tag);
            auto&& n = to_sstring(d.second);
            m->append(to_sstring(n.size()));
            m->append_static(msg_crlf);
            m->append(n);
            m->append_static(msg_crlf);
        }
    }
    return make_ready_future<scattered_message_ptr>(reply { foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m) });
}

static future<reply> build(std::vector<std::tuple<bytes, double, double, double, double>>& u, int flags)
{
    auto m = make_lw_shared<scattered_message<char>>();
    m->append_static(msg_sigle_tag);
    m->append(std::move(to_sstring(u.size())));
    m->append_static(msg_crlf);
    int temp = 1, temp2 = 2;
    bool wd = flags & GEORADIUS_WITHDIST;
    bool wh = flags & GEORADIUS_WITHHASH;
    bool wc = flags & GEORADIUS_WITHCOORD;
    if (wd) temp++;
    if (wh) temp++;
    if (wc) temp++;
    for (size_t i = 0; i < u.size(); ++i) {
        m->append_static(msg_sigle_tag);
        m->append(std::move(to_sstring(temp)));
        m->append_static(msg_crlf);

        //key
        bytes& key = std::get<0>(u[i]);
        m->append_static(msg_batch_tag);
        m->append(to_sstring(key.size()));
        m->append_static(msg_crlf);
        m->append(std::move(key));
        m->append_static(msg_crlf);
        //dist
        if (wd) {
            double dist = std::get<2>(u[i]);
            geo::from_meters(dist, flags);
            auto&& n2 = to_sstring(dist);
            m->append_static(msg_batch_tag);
            m->append(to_sstring(n2.size()));
            m->append_static(msg_crlf);
            m->append(std::move(n2));
            m->append_static(msg_crlf);
        }
        //coord
        if (wc) {
            m->append_static(msg_sigle_tag);
            m->append(std::move(to_sstring(temp2)));
            m->append_static(msg_crlf);
            auto&& n1 = to_sstring(std::get<3>(u[i]));
            m->append_static(msg_batch_tag);
            m->append(to_sstring(n1.size()));
            m->append_static(msg_crlf);
            m->append(std::move(n1));
            m->append_static(msg_crlf);
            auto&& n2 = to_sstring(std::get<4>(u[i]));
            m->append_static(msg_batch_tag);
            m->append(to_sstring(n2.size()));
            m->append_static(msg_crlf);
            m->append(std::move(n2));
            m->append_static(msg_crlf);
        }
        //hash
        if (wh) {
            double& score = std::get<1>(u[i]);
            bytes hashstr;
            geo::encode_to_geohash_string(score, hashstr);
            m->append_static(msg_batch_tag);
            m->append(to_sstring(hashstr.size()));
            m->append_static(msg_crlf);
            m->append(std::move(hashstr));
            m->append_static(msg_crlf);
        }
    }
    return make_ready_future<scattered_message_ptr>(reply { foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m) });
}
*/
}; // end of class
}
