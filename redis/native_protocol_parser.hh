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
#include "core/temporary_buffer.hh"
#include "util/eclipse.hh"
#include <algorithm>
#include <memory>
#include <cassert>
#include <cstring>
#include <experimental/optional>
#include "core/future.hh"
#include "exceptions/exceptions.hh"
#include "redis/request.hh"
#include "protocol_parser.hh"
namespace redis {

class native_protocol_parser : public protocol_parser::impl {
    static constexpr size_t MAX_INLINE_BUFFER_SIZE = 1024 * 64; // 64K
    request _req;
    char* find_first_nonnumeric(char* begin, char* end) const;
    uint32_t convert_to_number(char* begin, char* end) const;
public:
    native_protocol_parser() {}
    virtual ~native_protocol_parser() {}
    virtual void init();
    virtual char* parse(char* p, char* limit, char* eof);
    virtual redis::request& get_request() { 
        std::transform(_req._command.begin(), _req._command.end(), _req._command.begin(), ::tolower);
        return _req;
    }
};

}
