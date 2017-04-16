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
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/timer-set.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/units.hh"
#include "core/distributed.hh"
#include "core/vector-data-sink.hh"
#include "core/bitops.hh"
#include "core/slab.hh"
#include "core/align.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include "cache.hh"
#include "dict_lsa.hh"
namespace redis {
using scattered_message_ptr = foreign_ptr<lw_shared_ptr<scattered_message<char>>>;

class reply_builder final {
public:
static future<scattered_message_ptr> build(size_t size)
{
    auto m = make_lw_shared<scattered_message<char>>();
    m->append_static(msg_num_tag);
    m->append(to_sstring(size));
    m->append_static(msg_crlf);
    return make_ready_future<scattered_message_ptr>(foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m));
}
static future<> build_local(output_stream<char>& out, size_t size)
{
    auto m = make_lw_shared<scattered_message<char>>();
    m->append_static(msg_num_tag);
    m->append(to_sstring(size));
    m->append_static(msg_crlf);
    return out.write(std::move(*m));
}

static future<scattered_message_ptr> build(const sstring& message)
{
   auto m = make_lw_shared<scattered_message<char>>();
   m->append_static(message);
   return make_ready_future<scattered_message_ptr>(foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m));
}

inline static future<> build_local(output_stream<char>& out, const sstring& message)
{
   return out.write(message);
}

template<bool Key, bool Value>
static future<scattered_message_ptr> build(const cache_entry* e)
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
            return make_ready_future<scattered_message_ptr>(foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m));
        }
    }
    else {
        return reply_builder::build(msg_not_found);
    }
}

template<bool Key, bool Value>
static future<scattered_message_ptr> build(const std::vector<const dict_entry*>& entries)
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
        return make_ready_future<scattered_message_ptr>(foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m));
    }
    else {
        return reply_builder::build(msg_nil);
    }
}

static  future<> build_local(output_stream<char>& out, const std::vector<sstring>& entries)
{
    if (!entries.empty()) {
        auto m = make_lw_shared<scattered_message<char>>();
        m->append_static(msg_sigle_tag);
        m->append(std::move(to_sstring(entries.size())));
        m->append_static(msg_crlf);
        for (size_t i = 0; i < entries.size(); ++i) {
            const auto e = entries[i];
            m->append_static(msg_batch_tag);
            m->append(to_sstring(e.size()));
            m->append_static(msg_crlf);
            m->append(e);
            m->append_static(msg_crlf);
        }
        return out.write(std::move(*m));
    }
    else {
        return out.write(msg_nil);
    }
}

template<bool Key, bool Value>
static future<scattered_message_ptr> build(const dict_entry* e)
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
            return make_ready_future<scattered_message_ptr>(foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m));
        }
        return make_ready_future<scattered_message_ptr>(foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m));
    }
    else {
        return reply_builder::build(msg_nil);
    }
}

static future<scattered_message_ptr> build(const std::vector<const managed_bytes*>& data)
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
    return make_ready_future<scattered_message_ptr>(foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m));
}

static future<scattered_message_ptr> build(const managed_bytes& data)
{
    auto m = make_lw_shared<scattered_message<char>>();
    m->append_static(msg_batch_tag);
    m->append(to_sstring(data.size()));
    m->append_static(msg_crlf);
    m->append(sstring{reinterpret_cast<const char*>(data.data()), data.size()});
    m->append_static(msg_crlf);
    return make_ready_future<scattered_message_ptr>(foreign_ptr<lw_shared_ptr<scattered_message<char>>>(m));
}
};
}
