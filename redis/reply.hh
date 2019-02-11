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
#include <iomanip>
#include <sstream>
#include <functional>
#include <unordered_map>
#include <vector>
#include "bytes.hh"
#include  <experimental/vector>
namespace redis {
using namespace seastar;
using scattered_message_ptr = foreign_ptr<lw_shared_ptr<scattered_message<char>>>;
struct reply {
    reply() : _reply(nullptr) {}
    reply(scattered_message_ptr reply) : _reply(std::move(reply)) {}
    scattered_message_ptr _reply;
};
}
