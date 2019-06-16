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
#include "bytes.hh"
#include "seastar/core/temporary_buffer.hh"
#include "seastar/core/future.hh"
#include "util/eclipse.hh"
#include <algorithm>
#include <memory>
#include <cassert>
#include <cstring>
#include <experimental/optional>
#include "exceptions/exceptions.hh"
#include "request.hh"
namespace redis {
class protocol_parser final {
public:
    using unconsumed_remainder = std::experimental::optional<temporary_buffer<char>>;
    class impl {
    public:
        impl() {}
        virtual ~impl() {}
        virtual void init() = 0;
        virtual char* parse(char* p, char* limit, char* eof) = 0;
        virtual request& get_request() = 0;
    };
public:
    explicit protocol_parser(std::unique_ptr<impl> p) : _impl(std::move(p)) {}
    void init() { _impl->init(); }
    inline future<unconsumed_remainder> operator()(temporary_buffer<char> buf) {
        char* p = buf.get_write();
        char* pe = p + buf.size();
        char* eof = buf.empty() ? pe : nullptr;
        char* parsed = _impl->parse(p, pe, eof);
        if (parsed) {
            buf.trim_front(parsed - p);
            return make_ready_future<unconsumed_remainder>(std::move(buf));
        }
        return make_ready_future<unconsumed_remainder>();
    }
    inline request get_request() {
        return _impl->get_request();
    }
private:
    std::unique_ptr<impl> _impl;
};

extern protocol_parser make_ragel_protocol_parser();
extern protocol_parser make_native_protocol_parser();
}

