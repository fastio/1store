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
#include "common.hh"
#include "core/stream.hh"
#include "redis_protocol_parser.hh"
#include "net/packet-data-source.hh"
#include "net/packet-data-source.hh"

namespace redis {
class redis_service;
class redis_protocol {
private:
    redis_service& _redis;
    redis_protocol_parser _parser;
    args_collection _command_args;
public:
    redis_protocol(redis_service& redis);
    void prepare_request();
    void print_input()
    {
        std::cout << "{ \n" << _command_args._command_args_count << "\n";
        for (size_t i = 0; i < _command_args._command_args.size(); i++) {
            std::cout << i << "=> " << _command_args._command_args[i] << "\n";
        };
        std::cout << "}\n";
    }
    future<> handle(input_stream<char>& in, output_stream<char>& out);
};
}
