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



// Read required partition for write-before-read operations.
class prefetch_partition_builder {
    prefetched_list& _data;
    const query::partition_slice& _partition_slice;
    schema_ptr _schema;
    std::optional<bool> _left;
    long _start;
    long _end;
    bool _only_size = false;
    std::optional<long> _index = std::optional<long>();
    std::optional<bytes> _target = std::optional<bytes>();
private:
    void add_cell(prefetched_list::row& cells, const column_definition& def, const std::optional<query::result_bytes_view>& cell)
    {
        if (cell) {
            auto ctype = static_pointer_cast<const collection_type_impl>(def.type);
            if (!ctype->is_multi_cell()) {
                throw std::logic_error(sprint("cannot prefetch frozen collection: %s", def.name_as_text()));
            }
            auto map_type = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
            prefetched_list::cell_list list;
            cell->with_linearized([&] (bytes_view cell_view) {
                 auto v = map_type->deserialize(cell_view);
                 auto& n = value_cast<map_type_impl::native_type>(v);
                 _data._has_more = n.size() > 1;
                 // FIXME: build on filter & stop_iteration ?
                 if (_only_size) {
                    _data._size = n.size();
                 } else if (_left) {
                    auto& el = (*_left) ? n.front() : n.back();
                    list.emplace_back(prefetched_list::cell { el.first.serialize(), el.second.serialize() });
                 } else if (_index) {
                    auto l = *_index + static_cast<long>(n.size());
                    if (l < 0) {
                        throw std::logic_error(sprint("cannot prefetch index: %ld from %s", l, def.name_as_text()));
                    }
                    l = l % static_cast<long>(n.size());
                    auto& el = n[static_cast<size_t>(l)];
                    list.emplace_back(prefetched_list::cell { el.first.serialize(), el.second.serialize() });
                 } else if (_target) {
                    size_t count = 0, i = 0;
                    if (_start < 0) {
                        count = 0 - static_cast<size_t>(_start);
                        for (auto&& el : n | boost::adaptors::reversed) {
                            if (i >= count) {
                                break;
                            }
                            auto&& v = el.second.serialize();
                            if (v == *_target && i < count) {
                                ++i;
                                list.emplace_back(prefetched_list::cell { el.first.serialize(), std::move(v) });
                            }
                        }
                    } else {
                        count = static_cast<size_t>(_start) - 1;
                        for (auto&& el : n) {
                            if (count > 0 && i > count) break;
                            auto&& v = el.second.serialize();
                            if (v == *_target) {
                                ++i;
                                list.emplace_back(prefetched_list::cell { el.first.serialize(), std::move(v) });
                            }
                        }
                    }
                    _data._has_more = n.size() > list.size();
                 } else {
                     _end += static_cast<long>(n.size()); // safe.
                     if (_start < 0 || _end < 0 || _start > _end ) {
                        throw std::logic_error(sprint("cannot prefetch collection: %s, start: %ld, end: %ld", def.name_as_text(), _start, _end));
                     }
                     _end = _end % static_cast<long>(n.size());
                     size_t end = static_cast<size_t>(_end);
                     for (size_t i = static_cast<size_t>(_start); i <= end; ++i) {
                        auto&& el = n[i];
                        list.emplace_back(prefetched_list::cell { el.first.serialize(), el.second.serialize() });
                     }
                 }
                 cells = std::move(list);
            });
        }
    }
public:
    prefetch_partition_builder(prefetched_list& data, const query::partition_slice& ps, schema_ptr schema, bool left)
        : _data(data)
        , _partition_slice(ps)
        , _schema(std::move(schema))
        , _left(std::optional<bool>(left))
        , _start(0)
        , _end(0)
        , _only_size(false)
    {
    }
    prefetch_partition_builder(prefetched_list& data, const query::partition_slice& ps, schema_ptr schema, long start, long end)
        : _data(data)
        , _partition_slice(ps)
        , _schema(std::move(schema))
        , _left(std::optional<bool>())
        , _start(start)
        , _end(end)
        , _only_size(false)
    {
    }
    prefetch_partition_builder(prefetched_list& data, const query::partition_slice& ps, schema_ptr schema, only_size_tag)
        : _data(data)
        , _partition_slice(ps)
        , _schema(std::move(schema))
        , _left(std::optional<bool>())
        , _start(0)
        , _end(0)
        , _only_size(true)
    {
    }
    prefetch_partition_builder(prefetched_list& data, const query::partition_slice& ps, schema_ptr schema, long index)
        : _data(data)
        , _partition_slice(ps)
        , _schema(std::move(schema))
        , _left(std::optional<bool>())
        , _start(0)
        , _end(0)
        , _only_size(false)
        , _index(std::optional<long>(index))
    {
    }
    prefetch_partition_builder(prefetched_list& data, const query::partition_slice& ps, schema_ptr schema, const bytes& target, long count)
        : _data(data)
        , _partition_slice(ps)
        , _schema(std::move(schema))
        , _left(std::optional<bool>())
        , _start(count)
        , _end(0)
        , _only_size(false)
        , _target(std::optional<bytes>(target))
    {
    }
    void accept_new_partition(const partition_key& key, uint32_t row_count)
    {
    }

    void accept_new_partition(uint32_t row_count) {}

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row)
    {
        prefetched_list::row cells;
        auto row_iterator = row.iterator();
        for (auto&& id : _partition_slice.regular_columns) {
            add_cell(cells, _schema->regular_column_at(id), row_iterator.next_collection_cell());
        }
        _data._row = std::move(cells);
        _data._inited = true;
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {}
    void accept_partition_end(const query::result_row_view& static_row) {}
};

future<std::shared_ptr<prefetched_list>> prefetch_list_impl(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& raw_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    std::function<prefetch_partition_builder(std::shared_ptr<prefetched_list>, const schema_ptr, const query::partition_slice&)>&& bf)
{
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
    return proxy.query(schema, make_lw_shared(std::move(cmd)), std::move(partition_ranges), cl, {timeout, cs.get_trace_state()}).then([ps, schema, bf = std::move(bf)] (auto qr) {
        return query::result_view::do_with(*qr.query_result, [&] (query::result_view v) {
            auto pd = std::make_shared<prefetched_list>(schema);
            v.consume(ps, bf(pd, schema, ps));
            return pd;
        });
    });
}

future<std::shared_ptr<prefetched_list>> prefetch_partition_helper::prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& raw_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    bool left)
{
    return prefetch_list_impl(proxy, schema, raw_key, cl, timeout, cs,
        [left] (std::shared_ptr<prefetched_list> pd, const schema_ptr schema, const query::partition_slice& ps) { return prefetch_partition_builder(*pd, ps, schema, left); });
}

future<std::shared_ptr<prefetched_list>> prefetch_partition_helper::prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& raw_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    long start,
    long end)
{
    return prefetch_list_impl(proxy, schema, raw_key, cl, timeout, cs,
        [start, end] (std::shared_ptr<prefetched_list> pd, const schema_ptr schema, const query::partition_slice& ps) { return prefetch_partition_builder(*pd, ps, schema, start, end); });
}

future<std::shared_ptr<prefetched_list>> prefetch_partition_helper::prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& raw_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    only_size_tag)
{
    return prefetch_list_impl(proxy, schema, raw_key, cl, timeout, cs,
        [] (std::shared_ptr<prefetched_list> pd, const schema_ptr schema, const query::partition_slice& ps) { return prefetch_partition_builder(*pd, ps, schema, only_size_tag {}); });
}

future<std::shared_ptr<prefetched_list>> prefetch_partition_helper::prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& raw_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    long index)
{
    return prefetch_list_impl(proxy, schema, raw_key, cl, timeout, cs,
        [index] (std::shared_ptr<prefetched_list> pd, const schema_ptr schema, const query::partition_slice& ps) { return prefetch_partition_builder(*pd, ps, schema, index); });
}

future<std::shared_ptr<prefetched_list>> prefetch_partition_helper::prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& raw_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    bytes&& target,
    long count)
{
    return prefetch_list_impl(proxy, schema, raw_key, cl, timeout, cs,
        [target, count] (std::shared_ptr<prefetched_list> pd, const schema_ptr schema, const query::partition_slice& ps) { return prefetch_partition_builder(*pd, ps, schema, std::move(target), count); });
}

future<std::shared_ptr<prefetched_partition_simple>> prefetch_partition_helper::prefetch_simple(service::storage_proxy& proxy,
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
                return std::make_shared<prefetched_partition_simple>(schema, std::move(b));
            }
        }
        return std::make_shared<prefetched_partition_simple> (schema);
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
