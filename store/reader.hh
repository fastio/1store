#pragma once
#include "core/future.hh"
#include "core/shared_ptr.hh"
#include "utils/bytes.hh"
namespace store {

// A reader object allows to iterate on sstable, a set of sstables etc.    
class reader {
    class impl {
    public:
        impl () {}
        virtual ~impl () = 0;
        virtual future<> seek_to_first() = 0;
        virtual future<> seek_to_last() = 0;
        virtual future<> seek(bytes key) = 0;
        virtual future<> next() = 0;
        virtual partition current() const = 0;
    };
public:
    reader (std::unique_ptr<impl> i) : _impl(std::move(i)) {}
    ~reader () {}
    future<> seek_to_first() { return _impl->seek_to_first(); } 
    future<> seek_to_last() { return _impl->seek_to_last(); } 
    future<> seek(bytes key) { return _impl->seek(key); } 
    future<> next() { return _impl->next(); } 
    partition current() const { return _impl->current(); }
    bool eof() const { return _impl->eof(); }
};

class table;
struct block_handle;

extern lw_shared_ptr<reader> make_block_reader(lw_shared_ptr<table> ptable, block_handle index);
extern lw_shared_ptr<reader> make_sstable_reader(lw_shared_ptr<table> ptable);
extern lw_shared_ptr<reader> make_combined_sstables_reader(std::vector<lw_shared_ptr<table>> sstables);

}
