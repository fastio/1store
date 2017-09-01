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
namespace store {
using namespace seastar;
future<status> build_table(const std::string& dbname,
                    const options& options,
                    lw_shared_ptr<table_cache> table_cache,
                    lw_shared_ptr<iterator> iter,
                    lw_shared_ptr<file_meta_data> meta)
{
    meta->file_size = 0;
    iter->seek_to_first();
    auto fname = table_file_name(dbname, meta->number);
    if (iter->valid()) {
        auto writable_file = make_writable_file(fname);
        auto builder = make_lw_shared<table_builder>(options, writable_file);
        meta->smallest.decode_from(iter->key());
        return do_until([iter] { return iter->valid() == false; }, [this, writable_file, builder, meta] {
            slice key = iter->key();
            meta->largest.decode_from(key);
            return buidler->add(key, iter->value).then([iter] {
                iter->next();
                return make_ready_future<>();
            });
        }).finally ([this, builder, meta] {
            return builder->finish().then([this, builder, meta, table_cache] {
                meta->file_size = builder->file_size();
                assert(meta->file_size > 0);
                iterator* it = table_cache->new_iterator(read_options(), meta->number, meta->file_size);
                auto s = it->status();
                return make_ready_future<status> (s);
            });
        });
    }
    else {
        //FIXME: exception
        return make_ready_future<status> ({});
    }
}

}  // namespace store 
