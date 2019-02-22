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
#include "log.hh"

namespace redis {

static logging::logger log("command");
// Read required partition for write-before-read operations.
class prefetch_partition_builder {
    prefetched_partition_collection& _data;
    const query::partition_slice& _partition_slice;
    schema_ptr _schema;
private:
    void add_cell(prefetched_partition_collection::row& cells, const column_definition& def, const std::optional<query::result_bytes_view>& cell)
    {
        if (cell) {
            auto ctype = static_pointer_cast<const collection_type_impl>(def.type);
            if (!ctype->is_multi_cell()) {
                throw std::logic_error(sprint("cannot prefetch frozen collection: %s", def.name_as_text()));
            }
            auto map_type = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
            prefetched_partition_collection::cell_list list;
            cell->with_linearized([&] (bytes_view cell_view) {
                 auto v = map_type->deserialize(cell_view);
                 for (auto&& el : value_cast<map_type_impl::native_type>(v)) {
                    log.info(" name = {}, key = {}, value = {} ", make_sstring(def.name()), make_sstring(el.first.serialize()), make_sstring(el.second.serialize()));
                    list.emplace_back(prefetched_partition_collection::cell { el.first.serialize(), el.second.serialize() });
                 }
                 //cells.emplace(def.name(), std::move(list));
                 cells = std::move(list);
            });
        }
    }
public:
    prefetch_partition_builder(prefetched_partition_collection& data, const query::partition_slice& ps, schema_ptr schema)
        : _data(data)
        , _partition_slice(ps)
        , _schema(std::move(schema))
    {
    }
    void accept_new_partition(const partition_key& key, uint32_t row_count)
    {
        log.info("accept_new_partition");
        //_data._pkey = key;
    }

    void accept_new_partition(uint32_t row_count) {}

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row)
    {
        log.info(" accept_new_row rc size = {}", _partition_slice.regular_columns.size());
        prefetched_partition_collection::row cells;
        auto row_iterator = row.iterator();
        for (auto&& id : _partition_slice.regular_columns) {
        log.info(" accept_new_row cell");
            add_cell(cells, _schema->regular_column_at(id), row_iterator.next_collection_cell());
        }
        _data._row = std::move(cells);
        _data._inited = true;
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {}
    void accept_partition_end(const query::result_row_view& static_row) {}
};

future<std::unique_ptr<prefetched_partition_collection>> prefetch_partition_helper::prefetch_collection(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& raw_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    /*
    try {
        validate_for_read(keyspace(), cl);
    } catch (exceptions::invalid_request_exception& e) {
        throw exceptions::invalid_request_exception(sprint("Write operation require a read but consistency %s is not supported on reads", cl));
    }
    */

    static auto is_collection = [] (const column_definition& def) {
        return def.type->is_collection();
    };

    std::vector<query::clustering_range> ranges;
    ranges.emplace_back(query::clustering_range::make_open_ended_both_sides());
    std::vector<column_id> regular_cols;
    boost::range::push_back(regular_cols, schema->regular_columns()
        | boost::adaptors::filtered(is_collection) | boost::adaptors::transformed([] (auto&& col) { return col.id; }));
    query::partition_slice ps(
            ranges,
            std::move(std::vector<column_id> {}),
            std::move(regular_cols),
            query::partition_slice::option_set::of<
                query::partition_slice::option::send_partition_key,
                query::partition_slice::option::send_clustering_key,
                query::partition_slice::option::collections_as_maps>());
    query::read_command cmd(schema->id(), schema->version(), ps, std::numeric_limits<uint32_t>::max());
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(raw_key)));
    auto partition_range = dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*schema, std::move(pkey)));
    dht::partition_range_vector partition_ranges;
    partition_ranges.emplace_back(std::move(partition_range));
    return proxy.query(schema, make_lw_shared(std::move(cmd)), std::move(partition_ranges), cl, {timeout, cs.get_trace_state()}).then([ps, schema] (auto qr) {
        return query::result_view::do_with(*qr.query_result, [&] (query::result_view v) {
            auto pd = std::make_unique<prefetched_partition_collection>(schema);
            v.consume(ps, prefetch_partition_builder(*pd, ps, schema));
            return pd;
        });
    });
}

future<std::unique_ptr<prefetched_partition_simple>> prefetch_partition_helper::prefetch_simple(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& raw_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    // find the schema.
    //auto& db = proxy.get_db().local();
    //auto schema = db.find_schema(db::system_keyspace::redis::NAME, db::system_keyspace::redis::SIMPLE_OBJECTS);
    // create the read command.
    auto full_slice = partition_slice_builder(*schema).build();
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(raw_key)));
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(),
        full_slice, std::numeric_limits<int32_t>::max(), gc_clock::now(), tracing::make_trace_info(cs.get_trace_state()), query::max_partitions, utils::UUID(), cs.get_timestamp());

    // consume the result, and convert it to redis format.
    auto partition_range = dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*schema, std::move(pkey)));
    dht::partition_range_vector partition_ranges;
    partition_ranges.emplace_back(std::move(partition_range));
    return proxy.query(schema, command, std::move(partition_ranges), cl, {timeout, /*cs.get_trace_state()*/ nullptr}).then([schema] (auto co_result) {
        const auto& q_result = co_result.query_result; 
        if (q_result && q_result->partition_count() && (*(q_result->partition_count()) > 0)) {
            auto full_slice = partition_slice_builder(*schema).build();
            auto result_s = query::result_set::from_raw_result(schema, full_slice, *(q_result));
            const auto& row = result_s.row(0);
            if (row.has(sstring("data"))) {
                const auto& data = row.get_data_value("data");
                auto b = utf8_type->decompose(data);
                return std::make_unique<prefetched_partition_simple>(schema, std::move(b));
            }
        }
        return std::make_unique<prefetched_partition_simple> (schema);
    });
}

future<bool> prefetch_partition_helper::exists(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& raw_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    auto full_slice = partition_slice_builder(*schema).build();
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(raw_key)));
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(),
        full_slice, std::numeric_limits<int32_t>::max(), gc_clock::now(), tracing::make_trace_info(cs.get_trace_state()), query::max_partitions, utils::UUID(), cs.get_timestamp());

    auto partition_range = dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*schema, std::move(pkey)));
    dht::partition_range_vector partition_ranges;
    partition_ranges.emplace_back(std::move(partition_range));
    return proxy.query(schema, command, std::move(partition_ranges), cl, {timeout, /*cs.get_trace_state()*/ nullptr}).then([schema] (auto co_result) {
        const auto& q_result = co_result.query_result; 
        return q_result && q_result->partition_count() && (*(q_result->partition_count()) > 0);
    });
}

future<> abstract_command::write_mutation(service::storage_proxy& proxy, const schema_ptr schema, const bytes& key, bytes&& data, db::consistency_level cl, db::timeout_clock::time_point timeout, service::client_state& cs)
{
    // construct the mutation.
    auto m = make_mutation(schema, key);
    const column_definition& column = *schema->get_column_definition("data");
    // empty clustering key.
    //auto data_cell = utf8_type->decompose(make_sstring(data));
    //m.set_clustered_cell(clustering_key::make_empty(), data_def, atomic_cell::make_live(*utf8_type, api::timestamp_clock::now().time_since_epoch().count(), std::move(data_cell)));
    auto cell = make_cell(schema, *(column.type.get()), data); 
    m.set_clustered_cell(clustering_key::make_empty(), column, std::move(cell));
    // call service::storage_proxy::mutate_automicly to apply the mutation.
    return proxy.mutate_atomically(std::vector<mutation> { std::move(m) }, cl, timeout, nullptr/*cs.get_trace_state()*/);
}

future<> abstract_command::write_mutation(service::storage_proxy& proxy, const schema_ptr schema, const bytes& key, partition_dead_tag, db::consistency_level cl, db::timeout_clock::time_point timeout, service::client_state& cs) 
{
    // construct the mutation.
    auto m = make_mutation(schema, key);
    m.partition().apply(tombstone { api::new_timestamp(), gc_clock::now() });
    return proxy.mutate_atomically(std::vector<mutation> { std::move(m) }, cl, timeout, cs.get_trace_state());
}
} // end of redis namespace
