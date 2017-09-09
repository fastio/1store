#include "store/table.hh"
#include "store/filter_policy.hh"
#include "store/options.hh"
#include "store/table/block.hh"
#include "store/table/filter_block.hh"
#include "store/table/format.hh"
#include "store/util/coding.hh"

namespace store {

struct sstable::rep {
  ~rep() {
    delete filter;
    delete [] filter_data;
    delete index_block;
  }

  options _options;
  status _status;
  lw_shared_ptr<file_reader> _file_reader;
  lw_shared_ptr<filter_block_reader> _filter;
  managed_bytes _fileter_data;
  lw_shared_ptr<block_cache> _cache;

  block_handle _metaindex_handle;  // Handle to metaindex_block: saved from footer
  lw_shared_ptr<block> _index_block;
  future<> read_meta(const footer& footer);
  future<> read_filter(const slice& filter_handle_value);
};

future<temporary_buffer<char>> read_block(lw_shared_ptr<file_random_access_reader> r, block_handle index) {
    r->seek(index.offset());
    return r->read_exactly(index.size()).then([r] (auto&& buffer) {
        return std::move(buffer);
    }).hanlde_exception([index] (auto e) {
        // log it and throw it.
        throw io_exception();
    });;
}

future<lw_shared_ptr<table>> table::open(bytes fname, sstable_options opts) {
    return open_file_dma(fname, open_flags::ro).then([this, options = std::move(opts)] (file file_) mutable {
        return file_.size().then([this, file_ = std::move(file_), opts = std::move(opts)] (uint64_t size) {
            if (size < footer::kEncodedLength) {
                return make_exception_future<>(lw_shared_ptr<table>({}));
            }
            auto f = make_checked_file(opts._read_error_handler, file_);
            auto r = make_lw_shared<file_random_access_reader>(std::move(f), size, opts.sstable_buffer_size);
            reader->seek(size - Footer::kEncodedLength);
            return r->read_exactly(Footer::kEncodedLength).then([this, r] (auto buffer) mutable {
                if (buffer.size() != footer::kEncodedLength) {
                    return make_exception_future<>(lw_shared_ptr<table>({}));
                }
                // read footer
                footer footer_;
                if (!footer_.decode_from(buffer.get(), buffer.size())) {
                    return make_exception_future<>(lw_shared_ptr<table>({}));
                }
                // read index block, and do not parse the index block now.
                auto fut = read_block(r, footer_.index_handle());
                return fut.then([this, r = std::move(r), _footer = std::move(_footer)] (auto&& data) {
                    auto block_handle_key = convert_to_handle_key(footer_.index_handle());
                    auto index_block = cache->find_and_create(std::move(block_handle_key));
                    auto rep_ = std::make_unique<table::rep>(r, footer_.metaindex_handle(), index_block);
                    auto table_instance = make_lw_shared<table>(rep_);
                    // read meta block
                    return rep_->read_meta(footer_.metaindex_handle()).then([this, table_instance] {
                        return make_ready_future<lw_shared_ptr<table>> ({table_instance});
                    });
                });
             });
        }).handle_exception([this, fname] (std::exception_ptr) {
            throw redis::io_exception();
        });
    });
}

future<> table::read_meta(lw_shared_ptr<file_random_access_reader> reader, block_handle metaindex_handle) {
    if (rep_->options.filter_policy == nullptr) {
      // Do not need any metadata
      return make_ready_future<>();
    }

    return read_block(reader, metaindex_handle).then([this] (auto&& key, auto&& data) {
        auto meta = make_lw_shared<block>(std::move(key), std::move(data));
        auto meta_block_reader = make_block_reader(meta, bytes_comparator());
        bytes key { "filter." + rep_->_options.filter_policy->name() };
        meta_block_reader->seek(key);
        auto data = meta_block_reader->current().data();
        block_handle filter_handle;
        if (filter_handle.decode_from(data)) {
            return read_block(reader, filter_handle).then([this] (auto&& data) mutable { 
                // we do not put filter block into cache.
                rep_->_filter = make_lw_shared<filter_block_reader> (rep_->_options._filter_policy, std::move(data));
                return make_ready_future<>();
            });
        }
        return make_ready_future<>();
    });
}

table::~table() {
}

class block_reader : public reader::impl {
    lw_shared_ptr<table> _table;
    lw_shared_ptr<block> _block;
    block_handle _index;
    size_t _buffer_size;
    lw_shared_ptr<file_random_access_reader> _reader;
    read_block_from_table();
    bool _eof = false;
public:
    block_reader(lw_shared_ptr<table> ptable, block_handle index, size_t buffer_size = 4096)
        : _ptable(ptable)
        , _block(nullptr)
        , _index(index)
        , _buffer_size(buffer_size)
        , _reader(nullptr)
    {
        auto index_handle_key = convert_to_handle_key(index);
        auto cached_block = _ptable->cache()->find(index_handle_key);
        if (cached_block != nullptr) {
            _block = cached_block;
        }
        else {
            _reader = make_lw_shared<file_random_access_reader>(_ptable->get_file(), _ptable->file_size(), opts.sstable_buffer_size);
        };
    }
    future<> seek_to_first() {
        return make_ready_future<>();
    }
    future<> seek_to_last() {
        return make_ready_future<>();
    }
    future<> seek(bytes key) {
        return make_ready_future<>();
    }
    future<> next() {
        return make_ready_future<>();
    }
    partition current() const {
        return _current;
    }
    bool eof() const {
        return _eof;
    }
};

lw_shared_ptr<reader> make_block_reader(lw_shared_ptr<table> ptable, block_handle index) {
    return make_lw_shared<reader>(std::make_unique<block_reader>(ptable, std::move(index)));
}

class sstable_reader : public reader::impl {
   lw_shared_ptr<table> _table;
   lw_shared_ptr<block_reader> _index_block_reader;
   lw_shared_ptr<block_reader> _data_block_reader;
   lw_shared_ptr<file_random_access_reader> _reader;
   partition _current;
public:
   sstable_reader(lw_shared_ptr<table> ptable) : _table(ptable) {}
   future<> seek_to_first() {
       return _index_block_reader->seek_to_first().then([this] {
            block_handle data_handle;
            data_handle.decode_from(_index_block_reader->current().data());
            auto handle_key = convert_to_handle_key(data_handle);
            auto b  = _table->cache()->find(handle_key);
            if (b != nullptr) {
                _data_block_reader = make_shared_ptr<block_reader>(b);
                return _data_block_reader.seek_to_first();
            }
            return read_block(_reader, data_handle).then([this, key = std::move(handle_key)] (auto&& data) {
                auto b = _table->cache()->create(std::move(key), std::move(data));
                 _data_block_reader = make_block_reader(b);
                return _data_block_reader->seek_to_first();
            });
       });
   }
   future<> seek_to_last() {
       return _index_block_reader->seek_to_last().then([this] {
            block_handle data_handle;
            data_handle.decode_from(_index_block_reader->current().data());
            auto handle_key = convert_to_handle_key(data_handle);
            auto b  = _table->cache()->find(handle_key);
            if (b != nullptr) {
                _data_block_reader = make_shared_ptr<block_reader>(b);
                return _data_block_reader.seek_to_last();
            }
            return read_block(_reader, data_handle).then([this, key = std::move(handle_key)] (auto&& data) {
                auto b = _table->cache()->create(std::move(key), std::move(data));
                 _data_block_reader = make_block_reader(b);
                return _data_block_reader->seek_to_last();
            });
       });
   }
   future<> seek(bytes key) {
       return _index_block_reader->seek(key).then([this, key] {
            block_handle data_handle;
            data_handle.decode_from(_index_block_reader->current().data());
            auto handle_key = convert_to_handle_key(data_handle);
            auto b  = _table->cache()->find(handle_key);
            if (b != nullptr) {
                _data_block_reader = make_shared_ptr<block_reader>(b);
                return _data_block_reader.seek(key);
            }
            return read_block(_reader, data_handle).then([this, key = std::move(handle_key)] (auto&& data) {
                auto b = _table->cache()->create(std::move(key), std::move(data));
                 _data_block_reader = make_block_reader(b);
                return _data_block_reader->seek(ikey);
            });
       });
   }
   future<> next() {
       assert(_initialized);
       return _data_block_reader->next().then([this] mutable {
           if (_data_block_reader->current().empty()) {
               return _index_block_reader->next().then([this] mutable {
                   block_handle data_handle;
                   data_handle.decode_from(_index_block_reader->current().data());
                   auto handle_key = convert_to_handle_key(data_handle);
                   auto b  = _table->cache()->find(handle_key);
                   if (b != nullptr) {
                       _data_block_reader = make_shared_ptr<block_reader>(b);
                       return _data_block_reader->seek_to_first();
                   }
                   return read_block(_reader, data_handle).then([this, key = std::move(handle_key)] (auto&& data) {
                       auto b = _table->cache()->create(std::move(key), std::move(data));
                        _data_block_reader = make_block_reader(b);
                       return _data_block_reader->seek_to_first();
                   });
               });
           }
           return make_ready_future<>();
       });
   }
   partition current() {
       return _current;
   }
   bool eof() {
       return _index_block_reader->eof() && _data_block_reader->eof();
   }
};

lw_shared_ptr<reader> make_sstable_reader(lw_shared_ptr<table> ptable) {
    return make_lw_shared<reader>(std::make_unique<block_reader>(ptable, std::move(index)));
}
}
