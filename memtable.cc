#include "memtable.hh"
#include "core/thread.hh"
namespace std { namespace experimental {} }
namespace seastar { namespace stdx = std::experimental; }
using namespace seastar;

namespace store {
memtable::memtable()
{
}

memtable::~memtable() {
    //revert_flushed_memory();
    clear();
}

uint64_t memtable::dirty_size() const {
    return occupancy().total_space();
}

void memtable::clear() noexcept {
    with_allocator(allocator(), [this] {
        _partitions.clear_and_dispose(current_deleter<memtable_entry>());
    });
}

future<> memtable::clear_gently() noexcept {
    return futurize_apply([this] {
        static thread_local seastar::thread_scheduling_group scheduling_group(std::chrono::milliseconds(1), 0.2);
        auto attr = seastar::thread_attributes();
        attr.scheduling_group = &scheduling_group;
        auto t = std::make_unique<seastar::thread>(attr, [this] {
            auto& alloc = allocator();
            auto p = std::move(_partitions);
            while (!p.empty()) {
                auto batch_size = std::min<size_t>(p.size(), 32);
                //auto dirty_before = dirty_size();
                with_allocator(alloc, [&] () noexcept {
                    while (batch_size--) {
                        p.erase_and_dispose(p.begin(), [&] (auto e) {
                            alloc.destroy(e);
                        });
                    }
                });
                //remove_flushed_memory(dirty_before - dirty_size());
                seastar::thread::yield();
            }
        });
        auto f = t->join();
        return f.then([t = std::move(t)] {});
    }).handle_exception([this] (auto e) {
        this->clear();
    });
}

future<> memtable::apply(cache_entry& e) {
    assert(write_enabled());
    /*
    // call lower_bound so we have a hint for the insert, just in case.
    auto i = _partitions.lower_bound(key, memtable_entry::compare());
    if (i == _partitions.end()) {
        memtable_entry* entry = current_allocator().construct<memtable_entry>();
        i = partitions.insert(i, *entry);
    } else {
        upgrade_entry(*i);
    }
    */
    return make_ready_future<>();
}


logalloc::occupancy_stats memtable::occupancy() const {
    return logalloc::region::occupancy();
}

size_t memtable::partition_count() const {
    return _partitions.size();
}

memtable_entry::memtable_entry(memtable_entry&& o) noexcept
    : _link()
    , _partition(std::move(o._partition))
{
    using container_type = memtable::partitions_type;
    container_type::node_algorithms::replace_node(o._link.this_ptr(), _link.this_ptr());
    container_type::node_algorithms::init(o._link.this_ptr());
}
}
