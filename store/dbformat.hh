// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <stdio.h>
#include "store/comparator.hh"
#include "store/filter_policy.hh"
#include "store/slice.hh"
#include "store/table_builder.hh"
#include "store/util/coding.hh"
#include "utils/bytes.hh"

namespace store {

// Grouping of constants.  We may want to make some of these
// parameters set via options.
namespace config {
static const int kNumLevels = 7;

// Level-0 compaction is started when we hit this many files.
static const int kL0_CompactionTrigger = 4;

// Soft limit on number of level-0 files.  We slow down writes at this point.
static const int kL0_SlowdownWritesTrigger = 8;

// Maximum number of level-0 files.  We stop writes at this point.
static const int kL0_StopWritesTrigger = 12;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
static const int kMaxMemCompactLevel = 2;

// Approximate gap in bytes between samples of data read during iteration.
static const int kReadBytesPeriod = 1048576;

}  // namespace config

class internal_key;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
enum value_type {
  kTypeDeletion = 0x0,
  kTypeValue = 0x1
};
// kvalue_typeForSeek defines the value_type that should be passed when
// constructing a Parsedinternal_key object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// value_type, not the lowest).
static const value_type kvalue_typeForSeek = kTypeValue;

typedef uint64_t sequence_number;

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
static const sequence_number kMaxsequence_number =
    ((0x1ull << 56) - 1);

struct parsed_internal_key {
  slice user_key;
  sequence_number sequence;
  value_type type;

  parsed_internal_key() { }  // Intentionally left uninitialized (for speed)
  parsed_internal_key(const slice& u, const sequence_number& seq, value_type t)
      : user_key(u), sequence(seq), type(t) { }
  bytes debug_string() const;
};

// Return the length of the encoding of "key".
inline size_t internal_key_encoding_length(const parsed_internal_key& key) {
  return key.user_key.size() + 8;
}

// Append the serialization of "key" to *result.
extern void append_internal_key(bytes* result,
                              const parsed_internal_key& key);

// Attempt to parse an internal key from "internal_key".  On success,
// stores the parsed data in "*result", and returns true.
//
// On error, returns false, leaves "*result" in an undefined state.
extern bool parse_internal_key(const slice& internal_key,
                             parsed_internal_key* result);

// Returns the user key portion of an internal key.
inline slice extract_user_key(const slice& internal_key) {
  assert(internal_key.size() >= 8);
  return slice(internal_key.data(), internal_key.size() - 8);
}

inline value_type extract_value_type(const slice& internal_key) {
  assert(internal_key.size() >= 8);
  const size_t n = internal_key.size();
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  unsigned char c = num & 0xff;
  return static_cast<value_type>(c);
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
class internal_key_comparator : public comparator {
 private:
  const comparator* user_comparator_;
 public:
  explicit internal_key_comparator(const comparator* c) : user_comparator_(c) { }
  virtual const char* name() const;
  virtual int compare(const slice& a, const slice& b) const;
  virtual void find_shortest_separator(
      bytes* start,
      const slice& limit) const;
  virtual void find_short_successor(bytes* key) const;

  const comparator* user_comparator() const { return user_comparator_; }

  int compare(const internal_key& a, const internal_key& b) const;
};

// Filter policy wrapper that converts from internal keys to user keys
class internal_filter_policy : public filter_policy {
 private:
  const filter_policy* const user_policy_;
 public:
  explicit internal_filter_policy(const filter_policy* p) : user_policy_(p) { }
  virtual const char* name() const;
  virtual void create_filter(const slice* keys, int n, bytes* dst) const;
  virtual bool key_may_match(const slice& key, const slice& filter) const;
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an internal_keyComparator.
class internal_key {
 private:
  bytes rep_;
 public:
  internal_key() { }   // Leave rep_ as empty to indicate it is invalid
  internal_key(const slice& user_key, sequence_number s, value_type t) {
    append_internal_key(&rep_, parsed_internal_key(user_key, s, t));
  }

  void decode_from(const slice& s) { rep_ = {s.data(), s.size()}; }
  slice encode() const {
    assert(!rep_.empty());
    return { rep_.data(), rep_.size() };
  }

  slice user_key() const { return extract_user_key({ rep_.data(), rep_.size() }); }

  void set_from(const parsed_internal_key& p) {
    rep_ = {};
    append_internal_key(&rep_, p);
  }

  void clear() { rep_ = {}; }

  bytes debug_string() const;
};

inline int internal_key_comparator::compare(
    const internal_key& a, const internal_key& b) const {
  return compare(a.encode(), b.encode());
}

inline bool parse_internal_key(const slice& internal_key,
                             parsed_internal_key* result) {
  const size_t n = internal_key.size();
  if (n < 8) return false;
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  unsigned char c = num & 0xff;
  result->sequence = num >> 8;
  result->type = static_cast<value_type>(c);
  result->user_key = slice(internal_key.data(), n - 8);
  return (c <= static_cast<unsigned char>(kTypeValue));
}

// A helper class useful for DBImpl::Get()
class lookup_key {
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  lookup_key(const slice& user_key, sequence_number sequence);

  ~lookup_key();

  // Return a key suitable for lookup in a MemTable.
  slice memtable_key() const { return slice(start_, end_ - start_); }

  // Return an internal key (suitable for passing to an internal iterator)
  slice internal_key() const { return slice(kstart_, end_ - kstart_); }

  // Return the user key
  slice user_key() const { return slice(kstart_, end_ - kstart_ - 8); }

 private:
  // We construct a char array of the form:
  //    klength  varint32               <-- start_
  //    userkey  char[klength]          <-- kstart_
  //    tag      uint64
  //                                    <-- end_
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an internal_key.
  const char* start_;
  const char* kstart_;
  const char* end_;
  char space_[200];      // Avoid allocation for short keys

  // No copying allowed
  lookup_key(const lookup_key&);
  void operator=(const lookup_key&);
};

inline lookup_key::~lookup_key() {
  if (start_ != space_) delete[] start_;
}

}  // namespace store 
