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

/** This protocol parser was inspired by the memcached app,
  which is the example in Seastar project.
**/

#include "redis/redis_command_code.hh"
#include <memory>
#include <iostream>
#include <algorithm>
#include <functional>
#include "bytes.hh"
#include "redis/protocol_parser.hh"
#include "redis/request.hh"
#include "log.hh"
using namespace seastar;
using namespace redis;
%%{

machine redis_resp_protocol;

access _fsm_;

action mark {
    g.mark_start(p);
}

action start_blob {
    g.mark_start(p);
    _size_left = _arg_size;
}
action start_command {
    g.mark_start(p);
    _size_left = _arg_size;
}

action advance_blob {
    auto len = std::min((uint32_t)(pe - p), _size_left);
    _size_left -= len;
    p += len;
    if (_size_left == 0) {
      _req._args.push_back(str());
      p--;
      fret;
    }
    p--;
}

action advance_command {
    auto len = std::min((uint32_t)(pe - p), _size_left);
    _size_left -= len;
    p += len;
    if (_size_left == 0) {
      _req._command = str();
      p--;
      fret;
    }
    p--;
}


crlf = '\r\n';
u32 = digit+ >{ _u32 = 0;}  ${ _u32 *= 10; _u32 += fc - '0';};
args_count = '*' u32 crlf ${_req._args_count = _u32 - 1;};
blob := any+ >start_blob $advance_blob;
command := any+ >start_command $advance_command;
arg = '$' u32 crlf ${ _arg_size = _u32;};

main := (args_count (arg @{fcall command; } crlf) (arg @{fcall blob; } crlf)+) ${_req._state = protocol_state::ok;};

prepush {
    prepush();
}

postpop {
    postpop();
}

}%%

namespace redis {

class bytes_builder {
    bytes _value;
    const char* _start = nullptr;
public:
    class guard;
public:
    bytes get() && {
        return std::move(_value);
    }
    void reset() {
        _value.reset();
        _start = nullptr;
    }
    friend class guard;
};

class bytes_builder::guard {
    bytes_builder& _builder;
    const char* _block_end;
public:
    guard(bytes_builder& builder, const char* block_start, const char* block_end)
        : _builder(builder), _block_end(block_end) {
        if (!_builder._value.empty()) {
            mark_start(block_start);
        }
    }
    ~guard() {
        if (_builder._start) {
            mark_end(_block_end);
        }
    }
    void mark_start(const char* p) {
        _builder._start = p;
    }
    void mark_end(const char* p) {
        if (_builder._value.empty()) {
            // avoid an allocation in the common case
            _builder._value = bytes(_builder._start, p);
        } else {
            _builder._value += bytes(_builder._start, p);
        }
        _builder._start = nullptr;
    }
};

class ragel_protocol_parser : public protocol_parser::impl {
    %% write data nofinal noprefix;
protected:
    int _fsm_cs;
    std::unique_ptr<int[]> _fsm_stack = nullptr;
    int _fsm_stack_size = 0;
    int _fsm_top;
    int _fsm_act;
    char* _fsm_ts;
    char* _fsm_te;
    bytes_builder _builder;

    void init_base() {
        _builder.reset();
    }
    void prepush() {
        if (_fsm_top == _fsm_stack_size) {
            auto old = _fsm_stack_size;
            _fsm_stack_size = std::max(_fsm_stack_size * 2, 16);
            assert(_fsm_stack_size > old);
            std::unique_ptr<int[]> new_stack{new int[_fsm_stack_size]};
            std::copy(_fsm_stack.get(), _fsm_stack.get() + _fsm_top, new_stack.get());
            std::swap(_fsm_stack, new_stack);
        }
    }
    void postpop() {}
    bytes get_str() {
        auto s = std::move(_builder).get();
        return std::move(s);
    }
public:
    redis::request _req;
    uint32_t _u32;
    uint32_t _arg_size;
    uint32_t _size_left;
public:
    ragel_protocol_parser() {}
    virtual ~ragel_protocol_parser() {}

    virtual void init() {
        init_base();
        _req._state = protocol_state::error;
        _req._args.clear();
        _req._args_count = 0;
        _size_left = 0;
        _arg_size = 0;
        %% write init;
    }

    virtual char* parse(char* p, char* pe, char* eof) {
        bytes_builder::guard g(_builder, p, pe);
        auto str = [this, &g, &p] {
            g.mark_end(p);
            auto s =  get_str();
            return s;
        };

        %% write exec;
        if (_req._state != protocol_state::error) {
            return p;
        }
        // error ?
        if (p != pe) {
            p = pe;
            return p;
        }
        return nullptr;
    }
    bool eof() const {
        return _req._state == protocol_state::eof;
    }
    virtual redis::request& get_request() { 
        std::transform(_req._command.begin(), _req._command.end(), _req._command.begin(), ::tolower);
        return _req;
    }
};

}

