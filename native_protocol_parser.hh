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
 *  Copyright (c) 2016-2026, Peng Jian, pengjian.uestc@gmail.com. All rights reserved.
 */

#pragma once
#include "utils/bytes.hh"
#include "core/temporary_buffer.hh"
#include "util/eclipse.hh"
#include <algorithm>
#include <memory>
#include <cassert>
#include <cstring>
#include <experimental/optional>
#include "core/future.hh"
#include "exceptions/exceptions.hh"
#include "request_wrapper.hh"

namespace redis {

class native_protocol_parser {
    static constexpr size_t MAX_INLINE_BUFFER_SIZE = 1024 * 64; // 64K
    bool _unfinished { false };
    request_wrapper _req; 
    using unconsumed_remainder = std::experimental::optional<temporary_buffer<char>>;
    inline char* parse(char* p, char* pe, char* eof)
    {
        auto s = p;
        auto begin = s, end = s;
        for (auto s = p; s <= pe; ++s) {
            if (*s == ' ') continue; 
            if (*s == '\n') {
                if (*(s - 1) != '\r') {
                    throw parse_exception("Protocol error: expected \\r");
                }
                end = s;
                if (static_cast<size_t>(end - begin) > MAX_INLINE_BUFFER_SIZE) {
                    throw parse_exception("Protocol error: too big inline request");
                }
                if (end <= begin) {
                    throw parse_exception("Protocol error: empty");
                }
                if (*begin == '*') {
                    if (_unfinished) {
                        throw parse_exception("Protocol error: invalid request");
                    }
                    _unfinished = true;
                    try {
                        _req._args_count = std::atoi(begin);
                    }
                    catch (const std::invalid_argument&) {
                        throw parse_exception("Protocol error: invalid request");
                    }
                }
                else {
                    _req._args.emplace_back(std::move(bytes(begin, end)));
                    begin =  s;
                    if (_req._args.size() == static_cast<size_t>(_req._args_count)) {
                        _unfinished = false;
                        break;
                    }
                }
            }
        }
        return begin;
    }
public:
    void init();
    inline future<unconsumed_remainder> operator()(temporary_buffer<char> buf) {
        char* p = buf.get_write();
        char* pe = p + buf.size();
        char* eof = buf.empty() ? pe : nullptr;
        char* parsed = parse(p, pe, eof);
        if (parsed) {
            buf.trim_front(parsed - p);
            return make_ready_future<unconsumed_remainder>(std::move(buf));
        }
        return make_ready_future<unconsumed_remainder>();
    }
};

}
