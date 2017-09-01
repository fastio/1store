// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "store/status.hh"
#include "core/future.hh"

namespace store {

struct options;
struct file_meta_data;

class iterator;
class table_cache;
class version_edit;

// Build a Table file from the contents of *iter.  The generated file
// will be named according to meta->number.  On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.
extern future<status> build_table(const std::string& dbname,
                         const options& options,
                         lw_shared_ptr<table_cache> table_cache,
                         lw_shared_ptr<iterator> iter,
                         lw_shared_ptr<file_meta_data> meta);

}  // namespace store 
