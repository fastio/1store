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
#include "core/sstring.hh"
#include "utils/managed_bytes.hh"
namespace redis {
class hll {
public:
    static size_t append(managed_bytes& data, const std::vector<sstring>& elements);
    static size_t count(managed_bytes& data);
    static size_t count(const uint8_t* merged_sources, size_t size);
    static size_t merge(managed_bytes& data, const uint8_t* merged_sources, size_t size); 
    static size_t merge(uint8_t* dest, size_t size, const sstring& merged_sources); 
};

}
