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
