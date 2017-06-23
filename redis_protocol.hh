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
#pragma once
#include "common.hh"
#include "core/stream.hh"
#include "redis_protocol_parser.hh"
#include "net/packet-data-source.hh"
#include "net/packet-data-source.hh"

namespace redis {
class redis_service;

class request_latency_tracer
{
    static constexpr const int SAMPLE_COUNT = 256;
    circular_buffer<double> _latencies;
    double _average_latency = 0;
    uint64_t _requests_served = 0;
    uint64_t _requests_serving = 0;
    uint64_t _requests_exception = 0;
    steady_clock_type::time_point _timestamp = steady_clock_type::now();
public:
    request_latency_tracer() {}
    ~request_latency_tracer() {}

    inline uint64_t number_exceptions() const {
        return _requests_exception;
    }

    inline double latency() const {
        return _average_latency;
    }

    inline uint64_t served() const {
        return _requests_served;
    }

    inline uint64_t serving() const {
        return _requests_serving;
    }

    inline void begin_trace_latency() {
        ++_requests_serving;
        _timestamp = steady_clock_type::now();
    }

    inline void incr_number_exceptions() {
        ++_requests_exception;
    }

    inline void end_trace_latency() {
        auto rt = static_cast<double>((steady_clock_type::now() - _timestamp).count() / 1000.0); //us
        _latencies.push_front(rt);
        if (_latencies.size() > SAMPLE_COUNT) {
            auto drop = _latencies.back();
            _latencies.pop_back();
            _average_latency -= (drop / SAMPLE_COUNT);
        }
        --_requests_serving;
        ++_requests_served;
        _average_latency += (rt / SAMPLE_COUNT);
    }
};

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
    future<> handle(input_stream<char>& in, output_stream<char>& out, request_latency_tracer& tracer);
};
}
