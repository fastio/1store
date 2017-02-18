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
#include "dict.hh"
#include <iomanip>
#include <sstream>
#include <unordered_map>
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/timer-set.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/units.hh"
#include "core/distributed.hh"
#include "core/vector-data-sink.hh"
#include "core/bitops.hh"
#include "core/slab.hh"
#include "core/align.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include <unistd.h>
#include <cstdlib>
namespace redis {

struct remote_origin_tag {
    template <typename T>
    static inline
    T move_if_local(T& ref) {
        return ref;
    }
};

struct local_origin_tag {
    template <typename T>
    static inline
    T move_if_local(T& ref) {
        return std::move(ref);
    }
};
using message_type = scattered_message<char>;
class storage {
public:
  storage(const sstring& name, dict* store) : name_(name), _store(store)
  {
  }
  virtual ~storage()
  {
  }
protected:
  sstring name_;
  dict* _store;
};
}
