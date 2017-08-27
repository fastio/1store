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
#include "core/stream.hh"
#include "redis_protocol_parser.hh"
#include "net/packet-data-source.hh"
#include "net/packet-data-source.hh"
#include "request_wrapper.hh"
using namespace seastar;
namespace redis {
//class redis_service;
class redis_protocol {
private:
    //redis_service& _redis;
    redis_protocol_parser _parser;
    request_wrapper _request {};
public:
    //redis_protocol(redis_service& redis);
    redis_protocol();
    void prepare_request();
    future<> handle(input_stream<char>& in, output_stream<char>& out);
};
}
