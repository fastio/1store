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
#include "common.hh"
namespace redis {
class bitmap
{
private:
    struct rep;
    rep* _rep;
public:
    bitmap();
    ~bitmap();
    size_t set_bit(size_t offset, bool value);
    bool   get_bit(size_t offset);
    size_t bit_count(long start, long end);
    size_t size();
};
}
