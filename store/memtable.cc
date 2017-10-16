#include "store/memtable.hh"
#include "stdx.hh"

namespace store {
memtable::memtable()
{
}

memtable::memtable(dirty_memory_manager& dmm)
    : logalloc::region(dmm.region_group())
    , _dirty_mgr(dmm)
{
}

static thread_local dirty_memory_manager mgr_for_tests;

memtable::~memtable() {
    revert_flushed_memory();
    clear();
}

uint64_t memtable::dirty_size() const {
    return occupancy().total_space();
}

void memtable::clear() noexcept {
    auto dirty_before = dirty_size();
    with_allocator(allocator(), [this] {
        partitions.clear_and_dispose(current_deleter<memtable_entry>());
    });
    remove_flushed_memory(dirty_before - dirty_size());
}

future<> memtable::clear_gently() noexcept {
    return futurize_apply([this] {
        static thread_local seastar::thread_scheduling_group scheduling_group(std::chrono::milliseconds(1), 0.2);
        auto attr = seastar::thread_attributes();
        attr.scheduling_group = &scheduling_group;
        auto t = std::make_unique<seastar::thread>(attr, [this] {
            auto& alloc = allocator();
            auto p = std::move(partitions);
            while (!p.empty()) {
                auto batch_size = std::min<size_t>(p.size(), 32);
                auto dirty_before = dirty_size();
                with_allocator(alloc, [&] () noexcept {
                    while (batch_size--) {
                        p.erase_and_dispose(p.begin(), [&] (auto e) {
                            alloc.destroy(e);
                        });
                    }
                });
                remove_flushed_memory(dirty_before - dirty_size());
                seastar::thread::yield();
            }
        });
        auto f = t->join();
        return f.then([t = std::move(t)] {});
    }).handle_exception([this] (auto e) {
        this->clear();
    });
}


partition&
memtable::find_or_create_partition(const redis::decorated_key& key) {
    assert(!reclaiming_enabled());
    assert(write_enabled());
    // call lower_bound so we have a hint for the insert, just in case.
    auto i = partitions.lower_bound(key, memtable_entry::compare(_schema));
    if (i == partitions.end() || !key.equal(*_schema, i->key())) {
        memtable_entry* entry = current_allocator().construct<memtable_entry>(redis::decorated_key { key });
        i = partitions.insert(i, *entry);
        return entry->partition();
    } else {
        upgrade_entry(*i);
    }
    return i->partition();
}


void memtable::add_flushed_memory(uint64_t delta) {
    _flushed_memory += delta;
    _dirty_mgr.account_potentially_cleaned_up_memory(this, delta);
}

void memtable::remove_flushed_memory(uint64_t delta) {
    delta = std::min(_flushed_memory, delta);
    _flushed_memory -= delta;
    _dirty_mgr.revert_potentially_cleaned_up_memory(this, delta);
}

void memtable::on_detach_from_region_group() noexcept {
    revert_flushed_memory();
}

void memtable::revert_flushed_memory() noexcept {
    _dirty_mgr.revert_potentially_cleaned_up_memory(this, _flushed_memory);
    _flushed_memory = 0;
}

class flush_memory_accounter {
    memtable& _mt;
public:
    void update_bytes_read(uint64_t delta) {
        _mt.add_flushed_memory(delta);
    }
    explicit flush_memory_accounter(memtable& mt)
        : _mt(mt)
	{}
    ~flush_memory_accounter() {
        assert(_mt._flushed_memory <= _mt.occupancy().used_space());

        // Flushed the current memtable. There is still some work to do, like finish sealing the
        // SSTable and updating the cache, but we can already allow the next one to start.
        //
        // By erasing this memtable from the flush_manager we'll destroy the semaphore_units
        // associated with this flush and will allow another one to start. We'll signal the
        // condition variable to let them know we might be ready early.
        _mt._dirty_mgr.remove_from_flush_manager(&_mt);
    }
    void account_component(memtable_entry& e) {
        auto delta = _mt.allocator().object_memory_size_in_allocator(&e)
                     + e.external_memory_usage_without_rows();
        update_bytes_read(delta);
    }
    void account_component(partition_snapshot& snp) {
        update_bytes_read(_mt.allocator().object_memory_size_in_allocator(&*snp.version()));
    }
};


logalloc::occupancy_stats memtable::occupancy() const {
    return logalloc::region::occupancy();
}

size_t memtable::partition_count() const {
    return partitions.size();
}

memtable_entry::memtable_entry(memtable_entry&& o) noexcept
    : _link()
    , _schema(std::move(o._schema))
    , _key(std::move(o._key))
    , _pe(std::move(o._pe))
{
    using container_type = memtable::partitions_type;
    container_type::node_algorithms::replace_node(o._link.this_ptr(), _link.this_ptr());
    container_type::node_algorithms::init(o._link.this_ptr());
}

class memtable_reader : public reader::impl {
   lw_shared_ptr<memtable> _memtable;
   memtable::partitions_type::const_iterator _begin;
   memtable::partitions_type::const_iterator _end;
   memtable::partitions_type::const_iterator _current;
   bool _eof = false;
public:
   memtable_reader(lw_shared_ptr<memtable> mtable)
      : _memtable(mtable)
      , _begin (mtable->_partitions.cbegin())
      , _end (mtable->_partitions.cend())
      , _current(mtable->_partitions.cbegin())
      , _eof(_begin == _end)
   {
   }
   ~memtable_reader() {}

   future<> seek_to_first() {
       _current = _begin;
       return make_ready_future<>();
   }

   future<> seek_to_last() {
       _current = --_end;
       return make_ready_future<>();
   }

   future<> seek(bytes key) {
       // disable seek operation
       return make_exception_future<>();
   }

   future<> next() {
       _current++;
       if (_current == _end) {
           _eof = true;
       }
       return make_ready_future<>();
   }

   bool eof() const {
       return _eof;
   }

   partition current() const {
       return _current->partition();
   }
};

lw_shared_ptr<memtable_reader> make_memtable_reader(lw_shared_ptr<memtable> mtable) {
    assert(mtable->enable_write() == false);
    return make_lw_shared<memtable_reader> (mtable);
}

}
