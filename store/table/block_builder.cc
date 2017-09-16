// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// block_builder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

/**
 *
 * Modified by Peng Jian, pstack@163.com.
 *
 **/
#include "store/table/block_builder.hh"

#include <algorithm>
#include <assert.h>
#include "store/comparator.hh"
#include "store/util/coding.hh"

namespace store {

block_builder::block_builder(const block_options& options)
    :_options(options)
    ,_restarts{}
    ,_counter(0)
    ,_finished(false)
{
    assert(options._block_restart_interval >= 1);
    _restarts.push_back(0);       // First restart point is at offset 0
}

void block_builder::reset()
{
    _buffer = {};
    _restarts.clear();
    _restarts.push_back(0);       // First restart point is at offset 0
    _counter = 0;
    _finished = false;
    _last_key = {};
}

size_t block_builder::current_size_estimate() const
{
    return (_buffer.size() +                        // Raw data buffer
            _restarts.size() * sizeof(uint32_t) +   // Restart array
            sizeof(uint32_t));                      // Restart array length
}

const bytes& block_builder::finish()
{
    // Append restart array
    for (size_t i = 0; i < _restarts.size(); i++) {
      put_fixed32(_buffer, _restarts[i]);
    }
    put_fixed32(_buffer, _restarts.size());
    _finished = true;
    return _buffer;
}

void block_builder::add(const bytes& key, const bytes& value) {
    assert(!_finished);
    assert(_counter <= _options._block_restart_interval);
    assert(_buffer.empty() // No values yet?
            || _options._comparator.compare(key, _last_key) > 0);
    size_t shared = 0;
    if (_counter < _options._block_restart_interval) {
        // See how much sharing to do with previous string
        auto min_length = std::min(_last_key.size(), key.size());
        while ((shared < min_length) && (_last_key[shared] == key[shared])) {
            shared++;
        }
    } else {
        // Restart compression
        _restarts.push_back(_buffer.size());
        _counter = 0;
    }
    const size_t non_shared = key.size() - shared;

    // Add "<shared><non_shared><value_size>" to buffer_
    put_varint32(_buffer, shared);
    put_varint32(_buffer, non_shared);
    put_varint32(_buffer, value.size());

    // Add string delta to buffer_ followed by value
    _buffer.append(key.data() + shared, non_shared);
    _buffer.append(value.data(), value.size());

    // Update state
    _last_key.resize(shared);
    _last_key.append(key.data() + shared, non_shared);
    assert(_last_key == key);
    _counter++;
}

}
