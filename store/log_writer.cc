// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

//
// Modified by Peng Jian.
//
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
#include "store/log_writer.hh"
#include <stdint.h>
#include "store/util/coding.hh"
#include "store/util/crc32c.hh"
#include "store/priority_manager.hh"
#include "core/align.hh"
namespace store {

log_writer::log_writer(file dest)
    : dest_(std::move(dest))
    , pos_ (0)
{
}

future<> log_writer::write(flush_buffer fb) {
    return do_with(std::move(fb), [this] (auto& fb) {
        return repeat([this, &fb] () mutable {
            auto data = fb.data();
            auto size = fb.size();
            auto&& priority_class = get_local_commitlog_priority();
            return dest_.dma_write(pos_, data, size, priority_class).then([this, &fb] (auto s) {
                fb.update_flushed_size(s);
                s = align_down<size_t>(s, 4096);
                pos_ += s;

                if (fb.flushed_all()) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                return make_ready_future<stop_iteration>(stop_iteration::no);
            });
        }).finally ([this] {
             return dest_.flush().then([] {
                 return make_ready_future<>();
             });
        });
    });
}
}  // namespace store
