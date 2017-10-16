// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

/**
 * Modified by Peng Jian. 
 **/

#include "store/comparator.hh"
#include <algorithm>
#include <stdint.h>
#include "utils/bytes.hh"
namespace store {

namespace {

class bytewise_comparator : public comparator {
 public:
  bytewise_comparator() { }

  virtual bytes& name() const {
    static bytes _name("redis.bytewise_comparator");
    return _name;
  }
  virtual int compare(const bytes_view& a, const bytes_view& b) const { return a < b; } 
  virtual int compare(const bytes& a, const bytes_view& b) const { return bytes_view { a } < b; }
  virtual int compare(const bytes_view& a, const bytes& b) const { return a < bytes_view { b }; }
  virtual int compare(const bytes& a, const bytes& b) const { return a < b; }
};
}

const comparator& default_bytewise_comparator() {
    static thread_local bytewise_comparator _default_bytewise_comparator;
    return _default_bytewise_comparator;
}
}
