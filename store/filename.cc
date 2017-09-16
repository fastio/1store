// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <ctype.h>
#include <stdio.h>
#include "store/filename.hh"
#include "store/dbformat.hh"
#include "store/util/logging.hh"

namespace store {

// A utility routine: write "data" to the named file and Sync() it.
extern future<status> write_string_to_file_sync(const bytes_view data,
                                    const bytes& fname);

static bytes make_file_name(const bytes& name, uint64_t number,
                                const char* suffix) {
  char buf[100];
  snprintf(buf, sizeof(buf), "/%06llu.%s",
           static_cast<unsigned long long>(number),
           suffix);
  return name + buf;
}

bytes log_file_name(const bytes& name, uint64_t number) {
  assert(number > 0);
  return make_file_name(name, number, "log");
}

bytes table_file_name(const bytes& name, uint64_t number) {
  assert(number > 0);
  return make_file_name(name, number, "ldb");
}

bytes sst_table_file_name(const bytes& name, uint64_t number) {
  assert(number > 0);
  return make_file_name(name, number, "sst");
}

bytes descriptor_file_name(const bytes& dbname, uint64_t number) {
  assert(number > 0);
  char buf[100];
  snprintf(buf, sizeof(buf), "/MANIFEST-%06llu",
           static_cast<unsigned long long>(number));
  return dbname + buf;
}

bytes current_file_name(const bytes& dbname) {
  return dbname + "/CURRENT";
}

bytes lock_file_name(const bytes& dbname) {
  return dbname + "/LOCK";
}

bytes temp_file_name(const bytes& dbname, uint64_t number) {
  assert(number > 0);
  return make_file_name(dbname, number, "dbtmp");
}

// Owned _file_names have the form:
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/LOG
//    dbname/LOG.old
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst|ldb)
bool parse_file_name(const bytes& fname,
                   uint64_t* number,
                   file_type* type) {
  bytes_view rest{fname};
  bytes_view mainfest_prefix { fname.data(), fname.size() < 9 ? fname.size() : 9 };
  if (rest == "CURRENT") {
    *number = 0;
    *type = kCurrentFile;
  } else if (rest == "LOCK") {
    *number = 0;
    *type = kDBLockFile;
  } else if (rest == "LOG" || rest == "LOG.old") {
    *number = 0;
    *type = kInfoLogFile;
  } else if (mainfest_prefix == bytes_view {"MANIFEST-"}) {
    rest.remove_prefix(mainfest_prefix.size());
    uint64_t num;
    if (!consume_decimal_number(rest, num)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kDescriptorFile;
    *number = num;
  } else {
    // Avoid strtoull() to keep _file_name format independent of the
    // current locale
    uint64_t num;
    if (!consume_decimal_number(rest, num)) {
      return false;
    }
    bytes_view suffix = rest;
    if (suffix == bytes_view(".log")) {
      *type = kLogFile;
    } else if (suffix == bytes_view(".sst") || suffix == bytes_view(".ldb")) {
      *type = kTableFile;
    } else if (suffix == bytes_view(".dbtmp")) {
      *type = kTempFile;
    } else {
      return false;
    }
    *number = num;
  }
  return true;
}

future<> set_current_file(const bytes& dbname,
                      uint64_t descriptor_number) {
  // Remove leading "dbname/" and add newline to manifest file name
  /*
  bytes manifest = descriptor_file_name(dbname, descriptor_number);
  slice contents = manifest;
  assert(contents.starts_with(dbname + "/"));
  contents.remove_prefix(dbname.size() + 1);
  bytes tmp = temp_file_name(dbname, descriptor_number);
  return write_string_to_file_sync(contents.to_string() + "\n", tmp).then([this] (auto s) {
      if (s.ok()) {
        return  rename_file(tmp, current_file_name(dbname));
      }
      if (!s.ok()) {
        return delete_file(tmp);
      }
  });    
  */
    return make_ready_future<>();
}

}
