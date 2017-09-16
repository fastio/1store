// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same status must use
// external synchronization.

#pragma once

#include <string>
#include "utils/bytes.hh"
namespace store {

class status {
 public:
  // Create a success status.
  status() : state_(nullptr) { }
  ~status() { delete[] state_; }

  // Copy the specified status.
  status(const status& s);
  void operator=(const status& s);

  // Return a success status.
  static status OK() { return status(); }

  // Return error status of an appropriate type.
  static status not_found(const bytes_view& msg, const bytes_view& msg2 = bytes_view()) {
    return status(kNotFound, msg, msg2);
  }
  static status corruption(const bytes_view& msg, const bytes_view& msg2 = bytes_view()) {
    return status(kCorruption, msg, msg2);
  }
  static status not_supported(const bytes_view& msg, const bytes_view& msg2 = bytes_view()) {
    return status(kNotSupported, msg, msg2);
  }
  static status invalid_argument(const bytes_view& msg, const bytes_view& msg2 = bytes_view()) {
    return status(kInvalidArgument, msg, msg2);
  }
  static status io_error(const bytes_view& msg, const bytes_view& msg2 = bytes_view()) {
    return status(kIOError, msg, msg2);
  }

  // Returns true iff the status indicates success.
  bool ok() const { return (state_ == nullptr); }

  // Returns true iff the status indicates a NotFound error.
  bool is_not_found() const { return code() == kNotFound; }

  // Returns true iff the status indicates a Corruption error.
  bool is_corruption() const { return code() == kCorruption; }

  // Returns true iff the status indicates an IOError.
  bool is_io_error() const { return code() == kIOError; }

  // Returns true iff the status indicates a NotSupportedError.
  bool is_not_supportedError() const { return code() == kNotSupported; }

  // Returns true iff the status indicates an InvalidArgument.
  bool is_invalid_argument() const { return code() == kInvalidArgument; }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string to_string() const;

 private:
  // OK status has a nullptr state_.  Otherwise, state_ is a new[] array
  // of the following form:
  //    state_[0..3] == length of message
  //    state_[4]    == code
  //    state_[5..]  == message
  const char* state_;

  enum code_type {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5
  };

  code_type code() const {
    return (state_ == nullptr) ? kOk : static_cast<code_type>(state_[4]);
  }

  status(code_type c, const bytes_view& msg, const bytes_view& msg2);
  static const char* copy_state(const char* s);
};

inline status::status(const status& s) {
  state_ = (s.state_ == nullptr) ? nullptr : copy_state(s.state_);
}
inline void status::operator=(const status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (state_ != s.state_) {
    delete[] state_;
    state_ = (s.state_ == nullptr) ? nullptr : copy_state(s.state_);
  }
}
}
