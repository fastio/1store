// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Modified by Peng Jian.
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

#include <stdint.h>
#include "store/log_format.hh"
#include "utils/bytes.hh"
#include "core/future.hh"
#include "core/temporary_buffer.hh"
#include "core/file.hh"
#include "core/shared_ptr.hh"
#include "seastarx.hh"
#include "mutation.hh"
namespace store {

class flush_buffer {
    lw_shared_ptr<temporary_buffer<char>> _data;
    data_output _output;
    size_t _pending_size;
    size_t _written;
public:
    flush_buffer() : _data (nullptr), _output(nullptr, size_t(0)), _pending_size(0), _written(0)
    {
    }

    flush_buffer(char* data, size_t size)
        : _data(make_lw_shared<temporary_buffer<char>>(data, size, make_free_deleter(data)))
        , _output(data, size)
        , _pending_size(0)
        , _written(0)
    {
    }

    inline void skip(size_t n) {
        _pending_size += n;
    }

    inline size_t write(lw_shared_ptr<mutation> m) {
        return 0;
    }

    inline char* get_current() {
        return _data->get_write() + _pending_size;
    }

    inline size_t available_size() const {
        return _data->size() - _pending_size;
    }

    inline bool available() const {
        return !!_data;
    }

    inline bool flushed_all() const {
        return _written == _pending_size;
    }

    inline void update_flushed_size(size_t flushed) {
        _written += flushed;
    }

    inline char* data() {
        return _data->get_write() + _written;
    }

    inline size_t size() const {
        return _pending_size - _written;
    }

    inline void reset() {
        _pending_size = 0;
        _written = 0;
    }

    inline void close() {
    }
};

class log_writer {
 public:
  explicit log_writer(file dest);

  ~log_writer() {}

  future<> write(flush_buffer fb);

 private:
  file dest_;
  size_t pos_ = 0;          // the current pos of file
  // No copying allowed
  log_writer(const log_writer&) = delete;
  void operator=(const log_writer&) = delete;
};

}
