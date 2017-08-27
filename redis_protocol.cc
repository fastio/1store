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
#include "redis_protocol.hh"
#include <algorithm>
#include "util/log.hh"
#include "redis_command_code.hh"
namespace redis {

static seastar::logger rlog("proto");
redis_protocol::redis_protocol()
{
}

void redis_protocol::prepare_request()
{
    _request._args_count = _parser._args_count - 1;
    _request._args       = std::move(_parser._args_list);
}

future<> redis_protocol::handle(input_stream<char>& in, output_stream<char>& out)
{
    _parser.init();
    // NOTE: The command is handled sequentially. The parser will control the lifetime
    // of every parameters for command.
    return in.consume(_parser).then([this, &in, &out] () -> future<> {
        switch (_parser._state) {
            case protocol_state::eof:
            case protocol_state::error:
                return make_ready_future<>();

            case protocol_state::ok:
            {
            prepare_request();
            }
            default:
                return out.write("+OK\r\n");
        };
    }).then_wrapped([this, &in, &out] (auto&& f) -> future<> {
        try {
            f.get();
        } catch(...) {
        }
        return make_ready_future<>();
    });
}
}
