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
#pragma once
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <iomanip>
#include <sstream>
#include <unordered_map>
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
#include "redis_protocol_parser.hh"
#include <unistd.h>
#include <cstdlib>
#include "base.hh"
namespace redis {
class redis_commands;
class redis_service;
using item_ptr = foreign_ptr<lw_shared_ptr<item>>;
class redis_protocol {
private:
    redis_service& _redis;
    redis_protocol_parser _parser;
    args_collection _command_args;
protected:
    using this_type = redis_protocol;
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
                std::string s = std::to_string(item->int64());
                msg.append(to_sstring(s.size()));
                msg.append_static(msg_crlf);
                msg.append_static(s.c_str());
                msg.append_static(msg_crlf);
            } else if (item->type() == REDIS_RAW_ITEM || item->type() == REDIS_RAW_STRING) {
                msg.append(to_sstring(item->value_size()));
                msg.append_static(msg_crlf);
                msg.append_static(item->value());
                msg.append_static(msg_crlf);
            } else if (item->type() == REDIS_RAW_DOUBLE) {
                std::string s = std::to_string(item->Double());
                msg.append(to_sstring(s.size()));
                msg.append_static(msg_crlf);
                msg.append_static(s.c_str());
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
    static void  append_item(scattered_message<char>& msg, double c) {
        msg.append_static(msg_num_tag);
        msg.append_static(std::to_string(c));
        msg.append_static(msg_crlf);
    }
    static void  append_item(scattered_message<char>& msg, int c) {
        msg.append_static(msg_num_tag);
        msg.append(to_sstring(c));
        msg.append_static(msg_crlf);
    }
    template<bool key = false, bool value = true>
        static void  append_multi_items(scattered_message<char>& msg, std::vector<item_ptr>&& items) {
            msg.append(msg_sigle_tag);
            if (key && value)
                msg.append(std::move(to_sstring(items.size() * 2)));
            else
                msg.append(std::move(to_sstring(items.size())));
            msg.append(std::move(to_sstring(msg_crlf)));
            for (size_t i = 0; i < items.size(); ++i) {
                if (key) {
                    msg.append(msg_batch_tag);
                    msg.append(std::move(to_sstring(items[i]->key_size())));
                    msg.append(std::move(to_sstring(msg_crlf)));
                    sstring v{items[i]->key().data(), items[i]->key().size()};
                    msg.append(std::move(v));
                    msg.append(msg_crlf);
                }
                if (value) {
                    if (!items[i]) {
                        msg.append_static(msg_not_found);
                    }
                    else {
                        msg.append(msg_batch_tag);
                        if (items[i]->type() == REDIS_RAW_UINT64 || items[i]->type() == REDIS_RAW_INT64) {
                            std::string s = std::to_string(items[i]->int64());
                            msg.append_static(std::to_string(s.size()).c_str());
                            msg.append_static(msg_crlf);
                            msg.append_static(s.c_str());
                            msg.append_static(msg_crlf);
                        } else if (items[i]->type() == REDIS_RAW_ITEM || items[i]->type() == REDIS_RAW_STRING) {
                            msg.append(std::move(to_sstring(items[i]->value_size())));
                            msg.append(std::move(to_sstring(msg_crlf)));
                            sstring v{items[i]->value().data(), items[i]->value().size()};
                            msg.append(std::move(v));
                            msg.append(msg_crlf);
                        } else if (items[i]->type() == REDIS_RAW_DOUBLE) {
                            std::string s = std::to_string(items[i]->Double());
                            msg.append_static(std::to_string(s.size()).c_str());
                            msg.append_static(msg_crlf);
                            msg.append_static(s.c_str());
                            msg.append_static(msg_crlf);
                        } else {
                            msg.append_static(msg_type_err);
                        }
                    }
                }
            }
            msg.on_delete([item = std::move(items)] {});
        }
public:
    redis_protocol(redis_service& redis);
    void prepare_request();
    future<> handle(input_stream<char>& in, output_stream<char>& out);
};
}
