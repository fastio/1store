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
#include "redis_protocol.hh"
#include "redis.hh"
#include "base.hh"
#include <algorithm>
#include "redis_commands.hh"
namespace redis {

redis_protocol::redis_protocol(redis_service& redis) : _redis(redis)
{
}

void redis_protocol::prepare_request()
{
    _command_args._command_args_count = _parser._args_count - 1;
    _command_args._command = std::move(_parser._command);
    _command_args._command_args = std::move(_parser._args_list);
}

future<> redis_protocol::handle(input_stream<char>& in, output_stream<char>& out)
{
    _parser.init();
    return in.consume(_parser).then([this, &out] () -> future<> {
        switch (_parser._state) {
            case redis_protocol_parser::state::eof:
                return make_ready_future<>();

            case redis_protocol_parser::state::error:
                return out.write(msg_err);

            case redis_protocol_parser::state::ok:
            {
                prepare_request();
                if (_command_args._command_args_count <= 0 || _command_args._command_args.empty()) {
                    return out.write(msg_err);
                }
                sstring& command = _command_args._command;
                std::transform(command.begin(), command.end(), command.begin(), ::toupper);
                auto command_handler = redis_commands_ptr()->get(_command_args._command);
                return command_handler( _command_args, out);
            }
        };
        std::abort();
    }).then_wrapped([this, &out] (auto&& f) -> future<> {
        try {
            f.get();
        } catch (std::bad_alloc& e) {
            return out.write(msg_err);
        }
        return make_ready_future<>();
    });
}
}
