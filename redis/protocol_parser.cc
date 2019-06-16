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
#include "protocol_parser.hh"
#include "redis/ragel_protocol_parser.hh"
#include "redis/native_protocol_parser.hh"
namespace redis {

protocol_parser make_ragel_protocol_parser() {
    return protocol_parser { std::make_unique<ragel_protocol_parser> () };
}
protocol_parser make_native_protocol_parser() {
    return protocol_parser { std::make_unique<native_protocol_parser> () };
}
}
