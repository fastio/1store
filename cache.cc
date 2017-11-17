#include "cache.hh"
#include <limits>
#include "column_family.hh"
namespace redis {
cache_entry::cache_entry(cache_entry&& o) noexcept
    : _cache_link()
    , _type(o._type)
    , _key(std::move(_key))
    , _key_hash(std::move(o._key_hash))
{
    switch (_type) {
        case data_type::numeric:
            _u._float_number = std::move(o._u._float_number);
            break;
        case data_type::int64:
            _u._integer_number = std::move(o._u._integer_number);
            break;
        case data_type::bytes:
        case data_type::hll:
            _u._bytes = std::move(o._u._bytes);
            break;
        case data_type::list:
            _u._list = std::move(o._u._list);
            break;
        case data_type::dict:
        case data_type::set:
            _u._dict = std::move(o._u._dict);
            break;
        case data_type::sset:
            _u._sset = std::move(o._u._sset);
            break;
        default:
            break;
    }
    /*
    using container_type = boost::intrusive::unordered_set<cache_entry,
        boost::intrusive::member_hook<cache_entry, cache_entry::hook_type, &cache_entry::_cache_link>,
        boost::intrusive::power_2_buckets<true>,
        boost::intrusive::constant_time_size<true>>;
    container_type::node_algorithms::replace_node(o._cache_link.this_ptr(), _cache_link.this_ptr());
    container_type::node_algorithms::init(o._cache_link.this_ptr());
    */
}

future<> cache::flush_dirty_entry(store::column_family& cf)
{
    return make_ready_future<>();
}
}
