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
 */
#ifndef _REDIS_COMMANDS_H
#define _REDIS_COMMANDS_H
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <iomanip>
#include <sstream>
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
#include <unistd.h>
#include <cstdlib>
#include "base.hh"
namespace redis {
class sharded_redis;
class args_collection;
//class item;
using item_ptr = foreign_ptr<boost::intrusive_ptr<item>>;
using handler_type = std::function<future<> (args_collection&, output_stream<char>&)>;
class redis_commands {
private:
    using this_type = redis_commands;
    std::unordered_map<sstring, handler_type> _handlers;
    handler_type _dummy; 
    void regist_handler(sstring command, handler_type handler) {
        _handlers[command] = handler;
    }
    sharded_redis* _redis;
    static std::vector<sstring> _number_str;
    static void init_number_str_array() {
        for (size_t i = 0; i < 32; ++i) {
            sstring s(":");
            sstring n(std::to_string(i).c_str());
            s.append(n.c_str(), n.size());
            s.append(msg_crlf, 2);
            _number_str.emplace_back(std::move(s));
        }
    }
private:
    static void  append_item(scattered_message<char>& msg, sstring message) {
        msg.append_static(msg_batch_tag);
        msg.append_static(std::to_string(message.size()).c_str());
        msg.append_static(msg_crlf);
        msg.append_static(message);
        msg.append_static(msg_crlf);
    }
    static void  append_item(scattered_message<char>& msg, item_ptr item) {
        if (!item) {
            msg.append_static(msg_not_found);
        }
        else {
            msg.append_static(msg_batch_tag);
            if (item->type() == REDIS_RAW_UINT64 || item->type() == REDIS_RAW_INT64) {
                std::string s = std::to_string(item->uint64());
                msg.append_static(std::to_string(s.size()).c_str());
                msg.append_static(msg_crlf);
                msg.append_static(s.c_str());
                msg.append_static(msg_crlf);
            } else if (item->type() == REDIS_RAW_ITEM || item->type() == REDIS_RAW_STRING) {
                msg.append_static(std::to_string(item->value_size()).c_str());
                msg.append_static(msg_crlf);
                msg.append_static(item->value());
                msg.append_static(msg_crlf);
            } else {
                msg.append_static(msg_type_err);
            }
            msg.on_delete([item = std::move(item)] {});
        }
    }
    static void  append_item(scattered_message<char>& msg, uint64_t c) {
        msg.append_static(msg_num_tag);
        msg.append_static(std::to_string(c).c_str());
        msg.append_static(msg_crlf);
    }
    static void  append_item(scattered_message<char>& msg, int c) {
        if (c < 32) {
            msg.append_static(_number_str[c]);
        } else {
            msg.append_static(msg_num_tag);
            msg.append_static(std::to_string(c).c_str());
            msg.append_static(msg_crlf);
        }
    }
    static void  append_multi_items(scattered_message<char>& msg, std::vector<item_ptr> items) {
        msg.append_static("*");
        msg.append_static(to_sstring(items.size()));
        msg.append_static(msg_crlf);
        for (size_t i = 0; i < items.size(); ++i) {
            msg.append_static("$");
            msg.append_static(to_sstring(items[i]->value_size()));
            msg.append_static(msg_crlf);
            msg.append_static(items[i]->value());
            msg.append_static(msg_crlf);
        }
        msg.on_delete([item = std::move(items)] {});
    }
public:
    redis_commands();
    ~redis_commands() {} 
    void set_redis(sharded_redis* r) { _redis = r; }
    handler_type& get(sstring& command) {
        auto it = _handlers.find(command);
        if (it != _handlers.end()) {
            return it->second;
        }
        return _dummy;
    }
};
}
#endif
