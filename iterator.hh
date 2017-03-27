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
namespace redis {
class item;
template<typename ValueType>
class iterator {
public:
    iterator() {}
    virtual ~iterator() {}

    virtual bool valid() const = 0;

    virtual void seek_to_first() = 0;

    virtual void seek_to_last() = 0;

    virtual void seek(const sstring& key) = 0;

    virtual void next() = 0;

    virtual void prev() = 0;

    virtual  ValueType* value() const = 0;

    virtual int status() const = 0;


private:
    iterator(const iterator&);
    void operator=(const iterator&);
};

}  // namespace redis
