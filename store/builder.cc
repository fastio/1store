// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "store/builder.hh"

#include "store/filename.hh"
#include "store/dbformat.hh"
#include "store/table_cache.hh"
#include "store/version_edit.hh"
#include "store/store.hh"
#include "store/iterator.hh"
#include "core/future.hh"
#include "core/future-util.hh"
#include "seastarx.hh"
namespace store {
future<lw_shared_ptr<sstable>> build_table(const std::string& dbname,
                    const options& options,
                    lw_shared_ptr<table_cache> table_cache,
                    lw_shared_ptr<reader> iter,
                    lw_shared_ptr<file_meta_data> meta)
{
    meta->file_size = 0;
    iter->seek_to_first();
    auto fname = table_file_name(dbname, meta->number);
    if (iter->eof() == false) {
        auto writable_file = make_writable_file(fname);
        auto builder = make_lw_shared<table_builder>(options, writable_file);
        auto& current_partion = iter->current();
        meta->smallest.decode_from(current_partition.key());
        return do_until([iter] { return iter->eof() == false; }, [this, writable_file, builder, meta] {
            auto& current_partition = iter->current();
            auto& key = current_partition.key();
            auto& value = current_partition.value();
            meta->largest.decode_from(key);
            return buidler->add(key, value).then([iter] {
                iter->next();
                return make_ready_future<>();
            });
        }).finally ([this, builder, meta] {
            return builder->finish().then([this, builder, meta, table_cache] (auto stable) {
                meta->file_size = builder->file_size();
                assert(meta->file_size > 0);
                return stable;
            });
        });
    }
    else {
        // log error message.
        return make_exception_future<sstable> ({});
    }
}

}  // namespace store
