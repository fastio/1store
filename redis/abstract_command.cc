#include "abstract_command.hh"
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
#include "log.hh"

namespace redis {

static logging::logger log("command");

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

future<> abstract_command::write_mutation(service::storage_proxy& proxy, const schema_ptr schema, const bytes& key, bytes&& data, db::consistency_level cl, db::timeout_clock::time_point timeout, service::client_state& cs)
{
    auto m = make_mutation(schema, key);
    const column_definition& column = *schema->get_column_definition("data");
    auto cell = make_cell(schema, *(column.type.get()), data); 
    m.set_clustered_cell(clustering_key::make_empty(), column, std::move(cell));
    return proxy.mutate_atomically(std::vector<mutation> { std::move(m) }, cl, timeout, nullptr/*cs.get_trace_state()*/);
}

future<> abstract_command::write_mutation(service::storage_proxy& proxy, const schema_ptr schema, std::vector<std::pair<bytes, bytes>>&& datas, db::consistency_level cl, db::timeout_clock::time_point timeout, service::client_state& cs)
{
    return do_with(std::move(datas), [this, &proxy, schema, cl, timeout, &cs] (auto& datas) {
        return parallel_for_each(datas.begin(), datas.end(), [this, &proxy, schema, timeout, cl, &cs] (auto& kv) {
            auto m = make_mutation(schema, kv.first);
            const column_definition& column = *schema->get_column_definition("data");
            auto cell = make_cell(schema, *(column.type.get()), kv.second); 
            m.set_clustered_cell(clustering_key::make_empty(), column, std::move(cell));
            return proxy.mutate_atomically(std::vector<mutation> { std::move(m) }, cl, timeout, nullptr /*cs.get_trace_state() */);
        });
    });
}

future<> abstract_command::write_mutation(service::storage_proxy& proxy, const schema_ptr schema, const bytes& key, partition_dead_tag, db::consistency_level cl, db::timeout_clock::time_point timeout, service::client_state& cs) 
{
    auto m = make_mutation(schema, key);
    m.partition().apply(tombstone { api::new_timestamp(), gc_clock::now() });
    return proxy.mutate_atomically(std::vector<mutation> { std::move(m) }, cl, timeout, nullptr /*cs.get_trace_state()*/);
}

future<> abstract_command::write_list_mutation(service::storage_proxy& proxy, const schema_ptr schema, const bytes& key, std::vector<bytes>&& datas, db::consistency_level cl, db::timeout_clock::time_point timeout, service::client_state& cs, bool left)
{
    std::function<std::array<int8_t, 16>()> ug = nullptr;
    if (left) {
        auto time = precision_time::REFERENCE_TIME - (db_clock::now() - precision_time::REFERENCE_TIME);
        ug = [this, time = std::move(time)] () {
            auto&& pt = precision_time::get_next(time);
            auto uuid = utils::UUID_gen::get_time_UUID_bytes(pt._millis.time_since_epoch().count(), pt._nanos);
            return uuid;
        };
    } else {
        ug = [this] () {
            return utils::UUID_gen::get_time_UUID_bytes();
        };
    }
    auto m = make_mutation(schema, key);
    const column_definition& column = *schema->get_column_definition("data");
    auto&& ltype = static_cast<const list_type_impl*>(column.type.get());
    list_type_impl::mutation lm;
    
    lm.cells.reserve(datas.size());
    for (auto&& value : datas) {
        auto uuid = ug();
        lm.cells.emplace_back(bytes(uuid.data(), uuid.size()), make_cell(schema, *ltype->value_comparator(), std::move(value), atomic_cell::collection_member::yes));
    }
    if (left) {
        std::reverse(lm.cells.begin(), lm.cells.end());
    }
    m.set_cell(clustering_key::make_empty(), column, atomic_cell_or_collection::from_collection_mutation(ltype->serialize_mutation_form(std::move(lm))));
   
    return proxy.mutate_atomically(std::vector<mutation> { std::move(m) }, cl, timeout, nullptr /*cs.get_trace_state()*/);
}

future<> abstract_command::write_list_mutation(service::storage_proxy& proxy, const schema_ptr schema, const bytes& key, std::vector<std::pair<bytes, bytes>>&& datas, db::consistency_level cl, db::timeout_clock::time_point timeout, service::client_state& cs)
{
    auto m = make_mutation(schema, key);
    const column_definition& column = *schema->get_column_definition("data");
    auto&& ltype = static_cast<const list_type_impl*>(column.type.get());
    list_type_impl::mutation lm;
    
    lm.cells.reserve(datas.size());
    for (auto&& data : datas) {
        lm.cells.emplace_back(data.first, make_cell(schema, *ltype->value_comparator(), std::move(data.second), atomic_cell::collection_member::yes));
    }
    m.set_cell(clustering_key::make_empty(), column, atomic_cell_or_collection::from_collection_mutation(ltype->serialize_mutation_form(std::move(lm))));
   
    return proxy.mutate_atomically(std::vector<mutation> { std::move(m) }, cl, timeout, nullptr /*cs.get_trace_state()*/);
}

future<> abstract_command::write_list_dead_cell_mutation(service::storage_proxy& proxy, const schema_ptr schema, const bytes& key, std::vector<bytes>&& cell_keys, db::consistency_level cl, db::timeout_clock::time_point timeout, service::client_state& cs)
{
    auto m = make_mutation(schema, key);
    const column_definition& column = *schema->get_column_definition("data");
    auto&& ltype = static_cast<const list_type_impl*>(column.type.get());
    list_type_impl::mutation lm;
    
    lm.cells.reserve(cell_keys.size());
    for (auto&& ckey : cell_keys) {
        lm.cells.emplace_back(ckey, make_dead_cell());
    }    
    m.set_cell(clustering_key::make_empty(), column, atomic_cell_or_collection::from_collection_mutation(ltype->serialize_mutation_form(std::move(lm))));
   
    return proxy.mutate_atomically(std::vector<mutation> { std::move(m) }, cl, timeout, nullptr /*cs.get_trace_state()*/);
}

} // end of redis namespace
