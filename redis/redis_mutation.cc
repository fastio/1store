#include "redis_mutation.hh"
#include "cql3/query_options.hh"
#include "timeout_config.hh"
#include "service/client_state.hh"
#include "service/storage_proxy.hh"
#include "dht/i_partitioner.hh"
#include "partition_slice_builder.hh"
#include "query-result-reader.hh"
#include "gc_clock.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/indirected.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/range/adaptor/reversed.hpp>
#include <memory>
#include "seastar/core/sstring.hh"
#include "log.hh"
using namespace seastar;
namespace redis {

static logging::logger rlog("rm");
precision_time precision_time::get_next(db_clock::time_point millis) {
    auto next =  millis < _last._millis
        ? precision_time{millis, 9999}
    : precision_time{millis,
        std::max(0, _last._nanos - 1)};
    _last = next;
    return next;
}
constexpr const db_clock::time_point precision_time::REFERENCE_TIME;
thread_local precision_time precision_time::_last = {db_clock::time_point::max(), 0};


inline sstring make_sstring(bytes b) {
    return sstring{reinterpret_cast<const char*>(b.data()), b.size()};
}

namespace internal {
atomic_cell make_dead_cell() {
    return atomic_cell::make_dead(api::new_timestamp(), gc_clock::now());
}   

atomic_cell make_cell(const schema_ptr schema,
   const abstract_type& type,
   const fragmented_temporary_buffer::view& value,
   atomic_cell::collection_member cm = atomic_cell::collection_member::no)
{
   //auto ttl = _ttl;
   //if (ttl.count() <= 0) {
   auto ttl = schema->default_time_to_live();
   //}   

   if (ttl.count() > 0) {
       return atomic_cell::make_live(type, api::new_timestamp(), value, gc_clock::now() + ttl, ttl, cm);
   } else {
       return atomic_cell::make_live(type, api::new_timestamp(), value, cm);
   }   
}  

atomic_cell make_cell(const schema_ptr schema,
    const abstract_type& type,
    bytes_view value,
    atomic_cell::collection_member cm = atomic_cell::collection_member::no)
{
    return make_cell(schema, type, fragmented_temporary_buffer::view(value), cm);
}   

mutation make_mutation(seastar::lw_shared_ptr<redis_mutation<bytes>> r)
{
    // redis table's partition key is always text type.
    auto schema = r->schema();
    const column_definition& column = *schema->get_column_definition("data");
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(r->key())));
    auto m = mutation(schema, std::move(pkey));
    auto cell = make_cell(schema, *(column.type.get()), r->data()); 
    m.set_clustered_cell(clustering_key::make_empty(), column, std::move(cell));
    return std::move(m);
}

mutation make_mutation(seastar::lw_shared_ptr<redis_mutation<partition_dead_tag>> r)
{
    // redis table's partition key is always text type.
    auto schema = r->schema();
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(r->key())));
    auto m = mutation(schema, std::move(pkey));
    m.partition().apply(tombstone { api::new_timestamp(), gc_clock::now() });
    return std::move(m);
}

mutation make_mutation(seastar::lw_shared_ptr<list_mutation> r)
{
    std::function<std::array<int8_t, 16>()> ug = nullptr;
    if (r->data()._reversed) {
        auto time = precision_time::REFERENCE_TIME - (db_clock::now() - precision_time::REFERENCE_TIME);
        ug = [time = std::move(time)] () {
            auto&& pt = precision_time::get_next(time);
            auto uuid = utils::UUID_gen::get_time_UUID_bytes(pt._millis.time_since_epoch().count(), pt._nanos);
            return uuid;
        };
    } else {
        ug = [] () {
            return utils::UUID_gen::get_time_UUID_bytes();
        };
    }
    auto schema = r->schema();
    const column_definition& column = *schema->get_column_definition("data");
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(r->key())));
    auto m = mutation(schema, std::move(pkey));
    std::vector<bytes> cell_keys;
    cell_keys.reserve(r->data()._cells.size());
    for (size_t i = 0; i < r->data()._cells.size(); ++i) {
        auto uuid = ug();
        cell_keys.emplace_back(bytes(uuid.data(), uuid.size()));
    }
    if (r->data()._reversed) {
        std::reverse(cell_keys.begin(), cell_keys.end());
        std::reverse(r->data()._cells.begin(), r->data()._cells.end());
    }
    for (size_t i = 0; i < r->data()._cells.size(); ++i) {
        m.set_cell(clustering_key::from_single_value(*schema, bytes_type->decompose(data_value(std::move(cell_keys[i])))),
            column, make_cell(schema, *utf8_type, fragmented_temporary_buffer::view(r->data()._cells[i]), atomic_cell::collection_member::no));
    }
    /*
    for (auto&& value : r->data()._cells) {
        auto uuid = ug();
        cells.emplace_back(std::make_pair<clustering_key, atomic_cell>(std::move(clustering_key::from_single_value(*schema, bytes_type->decompose(make_sstring(bytes(uuid.data(), uuid.size()))))),
            std::move(make_cell(schema, *utf8_type, fragmented_temporary_buffer::view(value), atomic_cell::collection_member::no))));
        //m.set_cell(clustering_key::from_single_value(*schema, bytes_type->decompose(make_sstring(bytes(uuid.data(), uuid.size())))),
        //      column, make_cell(schema, *utf8_type, fragmented_temporary_buffer::view(value), atomic_cell::collection_member::no));
    }
    if (r->data()._reversed) {
        std::reverse(cells.begin(), cells.end());
    }
    for (auto&& cell : cells) {
        m.set_cell(std::move(cell.first), column, std::move(cell.second));
    }
    */
    return std::move(m);
}

mutation make_mutation(seastar::lw_shared_ptr<list_indexed_cells_mutation> r)
{
    auto schema = r->schema();
    const column_definition& column = *schema->get_column_definition("data");
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(r->key())));
    auto m = mutation(schema, std::move(pkey));
    for (auto&& data : r->data()._indexed_cells) {
        m.set_cell(clustering_key::from_single_value(*schema, bytes_type->decompose(data_value(*(data.first)))),
             column, make_cell(schema, *utf8_type, fragmented_temporary_buffer::view(*(data.second)), atomic_cell::collection_member::no));
    }
    return std::move(m);
}

mutation make_mutation(seastar::lw_shared_ptr<list_dead_cells_mutation> r)
{
    auto schema = r->schema();
    const column_definition& column = *schema->get_column_definition("data");
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(r->key())));
    auto m = mutation(schema, std::move(pkey));
    for (auto&& ckey : r->data()._cell_keys) {
        m.set_cell(clustering_key::from_single_value(*schema, utf8_type->decompose(data_value(*ckey))), column, make_dead_cell());
    }    
    return std::move(m);
}

mutation make_mutation(seastar::lw_shared_ptr<map_mutation> r)
{
    auto schema = r->schema();
    const column_definition& column = *schema->get_column_definition("data");
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(r->key())));
    auto m = mutation(schema, std::move(pkey));
    for (auto && e : r->data()) {
        m.set_cell(clustering_key::from_single_value(*schema, utf8_type->decompose(make_sstring(e.first))),
               column, make_cell(schema, *utf8_type, fragmented_temporary_buffer::view(e.second), atomic_cell::collection_member::no));
    }
    return std::move(m);
}

mutation make_mutation(seastar::lw_shared_ptr<map_dead_cells_mutation> r)
{
    auto schema = r->schema();
    const column_definition& column = *schema->get_column_definition("data");
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(r->key())));
    auto m = mutation(schema, std::move(pkey));
    for (auto&& e : r->data()._map_keys) {
        m.set_cell(clustering_key::from_single_value(*schema, utf8_type->decompose(make_sstring(e))),
               column, make_dead_cell());
    }    
    return std::move(m);
}

future<> write_mutation_impl(service::storage_proxy& proxy,
    std::vector<mutation>&& ms,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs) 
{
    return proxy.mutate_atomically(std::move(ms), cl, timeout, nullptr);
}

}

future<> write_mutations(
    service::storage_proxy& proxy,
    std::vector<seastar::lw_shared_ptr<redis_mutation<bytes>>> ms,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& client_state
)
{
    return do_with(std::move(ms), [&proxy, cl, timeout, &client_state] (auto& ms) {
        return parallel_for_each(ms.begin(), ms.end(), [&proxy, timeout, cl, &client_state] (auto& r) {
            auto m = internal::make_mutation(r);
            return internal::write_mutation_impl(proxy, std::vector<mutation> { std::move(m) }, cl ,timeout, client_state).finally([r] {});
        });
    });
}

} // end of redis namespace
