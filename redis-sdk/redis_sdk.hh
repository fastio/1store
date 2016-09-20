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
#include "core/print.hh"
#include "core/reactor.hh"
#include "core/sstring.hh"
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/distributed.hh"
#include "core/semaphore.hh"
#include "core/future-util.hh"
#include <chrono>
namespace pedis {
class redis_sdk {
private:
    sstring _host;
    uint16_t _port;
    semaphore _connected {0};
    semaphore _finished {0};
    semaphore _access {1};
public:
    redis_sdk(const sstring& host, uint16_t port) : _host(host), _port(port) {}
    ~redis_sdk() {}

    future<bool> set(const sstring& key, const sstring& value);
    future<sstring> get(const sstring& key);
    future<int> del(const sstring& key);
private:
    class connection {
    private:
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
        redis_response_parser _parser;
        redis_sdk* _sdk;
    public:
        connection(connected_socket&& fd, redis_sdk* sdk)
            : _fd(std::move(fd))
            , _read_buf(_fd.input())
            , _write_buf(_fd.output())
            , _sdk(sdk)
        {
        }

        template<typename T> 
        future<T> launch(sstring& content) {
            return _write_buf.write(content).then([this] {
                return _write_buf.flush();
            }).then([this] {
                 _parser.init();
                 return _read_buf.consume(_parser).then([this] {
                     if (_parser.eof()) {
                         return make_ready_future<void>();
                     }
                 });
            });
        }
    };
};
}
