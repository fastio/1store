// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Must not be included from any .h files to avoid polluting the namespace
// with macros.
#pragma once

#include <stdio.h>
#include <stdint.h>
#include "utils/bytes.hh"
namespace store {

class slice;

// Append a human-readable printout of "num" to *str
extern void append_number_to(bytes* str, uint64_t num);

// Append a human-readable printout of "value" to *str.
// Escapes any non-printable characters found in "value".
extern void append_escaped_string_to(bytes* str, const slice& value);

// Return a human-readable printout of "num"
extern bytes number_to_string(uint64_t num);

// Return a human-readable version of "value".
// Escapes any non-printable characters found in "value".
extern bytes escape_string(const bytes_view value);

// Parse a human-readable number from "*in" into *value.  On success,
// advances "*in" past the consumed number and sets "*val" to the
// numeric value.  Otherwise, returns false and leaves *in in an
// unspecified state.
extern bool consume_decimal_number(bytes_view& in, uint64_t& val);

}  // namespace store 
