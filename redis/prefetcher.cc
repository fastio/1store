#include "prefetcher.hh"
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

namespace redis {
class prefetched_map_builder {
    using data_type = prefetched_struct<std::vector<std::pair<std::optional<bytes>, std::optional<bytes>>>>;
    data_type& _data;
    const query::partition_slice& _partition_slice;
    const schema_ptr _schema;
    const fetch_options _option;
    std::function<bool(const column_definition& col, data_type&, const clustering_key&, bytes_view)> _filter = nullptr;
private:
    void add_cell(const column_definition& ckey_col, const clustering_key& ckey, const column_definition& col, const std::optional<query::result_atomic_cell_view>& cell)
    {
        using dtype = std::optional<bytes>;
        if (cell) {
            cell->value().with_linearized([this, &ckey_col, &ckey, &col] (bytes_view cell_view) {
                if (_filter == nullptr || _filter(col, _data, ckey, cell_view)) {
                    if (_option == fetch_options::keys) {
                        auto i = ckey.begin(*_schema);
                        auto&& ckey_data = ckey_col.type->deserialize_value(*i);
                        _data._data.emplace_back(std::make_pair<dtype, dtype>(dtype(std::move(ckey_data.serialize())), dtype()));
                    } else if (_option == fetch_options::values) {
                        auto&& data_data =  col.type->deserialize_value(cell_view);
                        _data._data.emplace_back(std::make_pair<dtype, dtype>(dtype(std::move(data_data.serialize())), dtype()));
                    } else {
                        auto i = ckey.begin(*_schema);
                        auto&& ckey_data = ckey_col.type->deserialize_value(*i);
                        auto&& data_data =  col.type->deserialize_value(cell_view);
                        _data._data.emplace_back(std::make_pair<dtype, dtype>(dtype(std::move(ckey_data.serialize())), dtype(std::move(data_data.serialize()))));
                    }
                    _data._inited = true;
                } 
            });
        }
    }
public:
    prefetched_map_builder(lw_shared_ptr<data_type> data, const schema_ptr schema, const query::partition_slice& ps, const fetch_options& option)
        : _data(*data)
        , _partition_slice(ps)
        , _schema(schema)
        , _option(option)
    {
    }
    void accept_new_partition(const partition_key& key, uint32_t row_count)
    {
    }

    void accept_new_partition(uint32_t row_count) {}

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row)
    {
        auto row_iterator = row.iterator();
        for (auto&& id : _partition_slice.regular_columns) {
            add_cell(*(_schema->get_column_definition(redis::CKEY_COLUMN_NAME)), key, _schema->regular_column_at(id), row_iterator.next_atomic_cell());
        }
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {}
    void accept_partition_end(const query::result_row_view& static_row) {}
};
future<map_return_type> prefetch_map_impl(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    std::vector<query::clustering_range>&& ranges,
    const fetch_options option,
    bool reversed,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    std::vector<column_id> regular_cols { schema->get_column_definition(redis::DATA_COLUMN_NAME)->id };
    query::partition_slice ps(
            ranges,
            std::move(std::vector<column_id> {}),
            std::move(regular_cols),
            query::partition_slice::option_set::of<
                query::partition_slice::option::send_partition_key,
                query::partition_slice::option::send_clustering_key,
                query::partition_slice::option::collections_as_maps>());
    if (reversed) {
        ps.set_reversed();
    }
    query::read_command cmd(schema->id(), schema->version(), ps, std::numeric_limits<uint32_t>::max(), gc_clock::now(), std::experimental::nullopt, 1);
    auto pkey = partition_key::from_single_value(*schema, key);
    auto partition_range = dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*schema, std::move(pkey)));
    dht::partition_range_vector partition_ranges;
    partition_ranges.emplace_back(std::move(partition_range));
    return proxy.query(schema, make_lw_shared(std::move(cmd)), std::move(partition_ranges), cl, {timeout, cs.get_trace_state()}).then([ps, schema, option] (auto qr) {
        return query::result_view::do_with(*qr.query_result, [&] (query::result_view v) {
            auto pd = make_lw_shared<prefetched_map_type>(schema);
            v.consume(ps, prefetched_map_builder(pd, schema, ps, option));
            return map_return_type { pd };
        });
    });
}

future<map_return_type> prefetch_map(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    const std::vector<bytes> ckeys,
    fetch_options option,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    std::vector<query::clustering_range> ranges;
    auto ckey_col = schema->get_column_definition(redis::CKEY_COLUMN_NAME);
    boost::range::push_back(ranges, ckeys | boost::adaptors::transformed([schema, ckey_col] (const auto& ckey) {
        return query::clustering_range::make_singular(clustering_key_prefix::from_single_value(*schema, ckey));
    }));
    return prefetch_map_impl(proxy, schema, key, std::move(ranges), option, false, cl, timeout, cs);
}

future<map_return_type> prefetch_map(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    fetch_options option,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    std::vector<query::clustering_range> ranges { query::full_clustering_range };
    return prefetch_map_impl(proxy, schema, key, std::move(ranges), option, false, cl, timeout, cs);
}

class prefetched_bytes_builder {
    using data_type = prefetched_struct<bytes>;
    data_type& _data;
    const query::partition_slice& _partition_slice;
    const schema_ptr _schema;
private:
    void add_cell(const column_definition& col, const std::optional<query::result_atomic_cell_view>& cell)
    {
        if (cell) {
            cell->value().with_linearized([this, &col] (bytes_view cell_view) {
                auto&& dv = col.type->deserialize_value(cell_view);
                auto&& d = dv.serialize();
                _data._data = std::move(d);
                _data._origin_size = _data._data.size();
                _data._inited = true;
            });
        }
    }
public:
    prefetched_bytes_builder(lw_shared_ptr<data_type> data, const schema_ptr schema, const query::partition_slice& ps)
        : _data(*data)
        , _partition_slice(ps)
        , _schema(schema)
    {
    }
    void accept_new_partition(const partition_key& key, uint32_t row_count)
    {
    }

    void accept_new_partition(uint32_t row_count) {}

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row)
    {
        auto row_iterator = row.iterator();
        for (auto&& id : _partition_slice.regular_columns) {
            add_cell(_schema->regular_column_at(id), row_iterator.next_atomic_cell());
        }
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {}
    void accept_partition_end(const query::result_row_view& static_row) {}
};

future<bytes_return_type> prefetch_simple(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    auto ps = partition_slice_builder(*schema).build();
    query::read_command cmd(schema->id(), schema->version(), ps, 1, gc_clock::now(), std::experimental::nullopt, 1);
    auto pkey = partition_key::from_single_value(*schema, key);
    auto partition_range = dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*schema, std::move(pkey)));
    dht::partition_range_vector partition_ranges;
    partition_ranges.emplace_back(std::move(partition_range));
    return proxy.query(schema, make_lw_shared(std::move(cmd)), std::move(partition_ranges), cl, {timeout, nullptr}).then([ps, schema] (auto qr) {
        return query::result_view::do_with(*qr.query_result, [&] (query::result_view v) {
            auto pd = make_lw_shared<prefetched_struct<bytes>>(schema);
            v.consume(ps, prefetched_bytes_builder(pd, schema, ps));
            return bytes_return_type { pd };
        });
    });
}

class prefetched_multi_struct_builder {
    using data_type = prefetched_struct<std::vector<std::pair<bytes, bytes>>>;
    data_type& _data; 
    const query::partition_slice& _partition_slice;
    const schema_ptr _schema;
    bytes _current;
public:
    prefetched_multi_struct_builder(lw_shared_ptr<data_type> data, const schema_ptr schema, const query::partition_slice& ps)
        : _data(*data)
        , _partition_slice(ps)
        , _schema(schema)
    {
    }
    void accept_new_partition(const partition_key& key, uint32_t row_count)
    {
        // only one partition key columns & only 1 regulair column.
        auto i = key.begin(*_schema);
        for (auto&& col : _schema->partition_key_columns()) {
            auto&& d = col.type->deserialize_value(*i);
            _current = d.serialize();
            ++i;
        }
    }

    void accept_new_partition(uint32_t row_count) {}

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row)
    {
        auto i = row.iterator(); 
        const column_definition& column = *_schema->get_column_definition("data");
        if (column.is_atomic() == false) {
            throw std::logic_error(sprint("The column: %s should be atomic", column.name_as_text()));
        }
        auto cell = i.next_atomic_cell();
        if (cell) {
            cell->value().with_linearized([&, this] (bytes_view bv) {
                auto&& d = column.type->deserialize_value(bv);
                auto&& v = d.serialize();
                _data._data.emplace_back(std::move(std::make_pair(std::move(_current), std::move(v))));
            });
            _data._inited = true;
        }
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {}
    void accept_partition_end(const query::result_row_view& static_row) {}
};

future<mbytes_return_type> prefetch_simple(service::storage_proxy& proxy,
    const schema_ptr schema,
    const std::vector<bytes>& keys,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    auto full_slice = partition_slice_builder(*schema).build();
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(),
        full_slice, std::numeric_limits<int32_t>::max(), gc_clock::now(), tracing::make_trace_info(cs.get_trace_state()), int32_t(keys.size()), utils::UUID(), cs.get_timestamp());
    auto partition_ranges = boost::copy_range<dht::partition_range_vector>(keys | boost::adaptors::transformed([schema] (auto& key) {
            auto pkey = partition_key::from_single_value(*schema, key);
            return dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*schema, std::move(pkey)));
    }));
    // consume the result, and convert it to redis format.
    return proxy.query(schema, command, std::move(partition_ranges), cl, {timeout, nullptr}).then([schema, full_slice] (auto qr) {
        return query::result_view::do_with(*qr.query_result, [&] (query::result_view v) {
            auto pd = make_lw_shared<prefetched_struct<std::vector<std::pair<bytes, bytes>>>>(schema);
            v.consume(full_slice, prefetched_multi_struct_builder(pd, schema, full_slice));
            return mbytes_return_type { pd };
        });
    });
}

future<map_return_type> prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    const fetch_options option,
    bool reversed,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    std::vector<query::clustering_range> ranges { query::full_clustering_range };
    return prefetch_map_impl(proxy, schema, key, std::move(ranges), option, reversed, cl, timeout, cs);
}

future<map_return_type> prefetch_set(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    std::vector<query::clustering_range> ranges { query::full_clustering_range };
    return prefetch_map_impl(proxy, schema, key, std::move(ranges), fetch_options::keys, false, cl, timeout, cs);
}

future<bool> exists(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    auto full_slice = partition_slice_builder(*schema).build();
    auto pkey = partition_key::from_single_value(*schema, key); 
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(),
        full_slice, std::numeric_limits<int32_t>::max(), gc_clock::now(), tracing::make_trace_info(cs.get_trace_state()), query::max_partitions, utils::UUID(), cs.get_timestamp());

    auto partition_range = dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*schema, std::move(pkey)));
    dht::partition_range_vector partition_ranges;
    partition_ranges.emplace_back(std::move(partition_range));
    return proxy.query(schema, command, std::move(partition_ranges), cl, {timeout, nullptr}).then([schema] (auto co_result) {
        const auto& q_result = co_result.query_result; 
        return q_result && q_result->partition_count() && (*(q_result->partition_count()) > 0);
    });
}

class prefetched_zset_builder {
    using data_type = prefetched_zset_type;
    data_type& _data;
    const query::partition_slice& _partition_slice;
    const schema_ptr _schema;
    const fetch_options _option;
    std::function<bool(const column_definition& col, data_type&, const clustering_key&, bytes_view)> _filter = nullptr;
private:
    void add_cell(const column_definition& ckey_col, const clustering_key& ckey, const column_definition& col, const std::optional<query::result_atomic_cell_view>& cell)
    {
        using dtype = std::optional<bytes>;
        using vtype = std::optional<double>;
        if (cell) {
            cell->value().with_linearized([this, &ckey_col, &ckey, &col] (bytes_view cell_view) {
                if (_filter == nullptr || _filter(col, _data, ckey, cell_view)) {
                    if (_option == fetch_options::keys) {
                        auto i = ckey.begin(*_schema);
                        auto&& ckey_data = ckey_col.type->deserialize_value(*i);
                        _data._data.emplace_back(std::make_pair<dtype, vtype>(dtype(std::move(ckey_data.serialize())), vtype()));
                    } else if (_option == fetch_options::values) {
                        auto&& data_data =  col.type->deserialize_value(cell_view);
                        _data._data.emplace_back(std::make_pair<dtype, vtype>(dtype(std::move(data_data.serialize())), vtype()));
                    } else {
                        auto i = ckey.begin(*_schema);
                        auto&& ckey_data = ckey_col.type->deserialize_value(*i);
                        auto&& data_data =  col.type->deserialize_value(cell_view);
                        _data._data.emplace_back(std::make_pair<dtype, vtype>(dtype(std::move(ckey_data.serialize())), vtype(value_cast<double>(data_data))));
                    }
                    _data._inited = true;
                } 
            });
        }
    }
public:
    prefetched_zset_builder(lw_shared_ptr<data_type> data, const schema_ptr schema, const query::partition_slice& ps, const fetch_options& option)
        : _data(*data)
        , _partition_slice(ps)
        , _schema(schema)
        , _option(option)
    {
    }
    void accept_new_partition(const partition_key& key, uint32_t row_count)
    {
    }

    void accept_new_partition(uint32_t row_count) {}

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row, const query::result_row_view& row)
    {
        auto row_iterator = row.iterator();
        for (auto&& id : _partition_slice.regular_columns) {
            add_cell(*(_schema->get_column_definition(redis::CKEY_COLUMN_NAME)), key, _schema->regular_column_at(id), row_iterator.next_atomic_cell());
        }
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {}
    void accept_partition_end(const query::result_row_view& static_row) {}
};

future<zset_return_type> prefetch_zset_impl(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    std::vector<query::clustering_range>&& ranges,
    const fetch_options option,
    bool reversed,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    std::vector<column_id> regular_cols { schema->get_column_definition(redis::DATA_COLUMN_NAME)->id };
    query::partition_slice ps(
            ranges,
            std::move(std::vector<column_id> {}),
            std::move(regular_cols),
            query::partition_slice::option_set::of<
                query::partition_slice::option::send_partition_key,
                query::partition_slice::option::send_clustering_key,
                query::partition_slice::option::collections_as_maps>());
    if (reversed) {
        ps.set_reversed();
    }
    query::read_command cmd(schema->id(), schema->version(), ps, std::numeric_limits<uint32_t>::max(), gc_clock::now(), std::experimental::nullopt, 1);
    auto pkey = partition_key::from_single_value(*schema, key);
    auto partition_range = dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*schema, std::move(pkey)));
    dht::partition_range_vector partition_ranges;
    partition_ranges.emplace_back(std::move(partition_range));
    return proxy.query(schema, make_lw_shared(std::move(cmd)), std::move(partition_ranges), cl, {timeout, cs.get_trace_state()}).then([ps, schema, option] (auto qr) {
        return query::result_view::do_with(*qr.query_result, [&] (query::result_view v) {
            auto pd = make_lw_shared<prefetched_zset_type>(schema);
            v.consume(ps, prefetched_zset_builder(pd, schema, ps, option));
            return zset_return_type { pd };
        });
    });
}

future<zset_return_type> prefetch_zset(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    const std::vector<bytes> ckeys,
    fetch_options option,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    std::vector<query::clustering_range> ranges;
    auto ckey_col = schema->get_column_definition(redis::CKEY_COLUMN_NAME);
    boost::range::push_back(ranges, ckeys | boost::adaptors::transformed([schema, ckey_col] (const auto& ckey) {
        return query::clustering_range::make_singular(clustering_key_prefix::from_single_value(*schema, ckey));
    }));
    return prefetch_zset_impl(proxy, schema, key, std::move(ranges), option, false, cl, timeout, cs);
}

future<zset_return_type> prefetch_zset(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    fetch_options option,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    std::vector<query::clustering_range> ranges { query::full_clustering_range };
    return prefetch_zset_impl(proxy, schema, key, std::move(ranges), option, false, cl, timeout, cs);
}
} // end of redis namespace
