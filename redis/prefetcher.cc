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

template<typename ContainerType>
class prefetched_struct_builder {
    using data_type = prefetched_struct<ContainerType>;
    data_type& _data;
    const query::partition_slice& _partition_slice;
    const schema_ptr _schema;
    std::function<bool(const column_definition& col, data_type&, bytes_view)> _filter;
private:
    void add_cell(data_type& data, const column_definition& col, const std::optional<query::result_bytes_view>& cell)
    {
        if (cell) {
            cell->with_linearized([this, &data, &col] (bytes_view cell_view) {
                auto inited = _filter(col, data, cell_view);
                data._inited = inited;
            });
        }
    }
public:
    prefetched_struct_builder(std::shared_ptr<data_type> data, const schema_ptr schema, const query::partition_slice& ps,
        std::function<bool (const column_definition&, data_type&, bytes_view)> filter)
        : _data(*data)
        , _partition_slice(ps)
        , _schema(schema)
        , _filter(filter)
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
            add_cell(_data, _schema->regular_column_at(id), row_iterator.next_collection_cell());
        }
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {}
    void accept_partition_end(const query::result_row_view& static_row) {}
};

template<typename ContainerType>
future<std::shared_ptr<prefetched_struct<ContainerType>>> prefetch_struct_impl(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    std::function<bool(const column_definition&, prefetched_struct<ContainerType>&, bytes_view)> filter)
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
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(key)));
    auto partition_range = dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*schema, std::move(pkey)));
    dht::partition_range_vector partition_ranges;
    partition_ranges.emplace_back(std::move(partition_range));
    return proxy.query(schema, make_lw_shared(std::move(cmd)), std::move(partition_ranges), cl, {timeout, cs.get_trace_state()}).then([ps, schema, filter] (auto qr) {
        return query::result_view::do_with(*qr.query_result, [&, filter] (query::result_view v) {
            auto pd = std::make_shared<prefetched_struct<ContainerType>>(schema);
            v.consume(ps, prefetched_struct_builder<ContainerType>(pd, schema, ps, filter));
            return pd;
        });
    });
}

template<>
class prefetched_struct_builder<bytes> {
    using data_type = prefetched_struct<bytes>;
    data_type& _data;
    const query::partition_slice& _partition_slice;
    const schema_ptr _schema;
    std::function<bool(const column_definition&, data_type&, bytes_view)> _filter;
private:
    void add_cell(data_type& data, const column_definition& col, const std::optional<query::result_atomic_cell_view>& cell)
    {
        if (cell) {
            cell->value().with_linearized([this, &data, &col] (bytes_view cell_view) {
                auto inited = _filter(col, data, cell_view);
                data._inited = inited;
            });
        }
    }
public:
    prefetched_struct_builder(std::shared_ptr<data_type> data, const schema_ptr schema, const query::partition_slice& ps,
        std::function<bool (const column_definition&, data_type&, bytes_view)> filter)
        : _data(*data)
        , _partition_slice(ps)
        , _schema(schema)
        , _filter(filter)
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
            add_cell(_data, _schema->regular_column_at(id), row_iterator.next_atomic_cell());
        }
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {}
    void accept_partition_end(const query::result_row_view& static_row) {}
};

template<>
future<std::shared_ptr<prefetched_struct<bytes>>> prefetch_struct_impl<bytes>(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    std::function<bool(const column_definition&, prefetched_struct<bytes>&, bytes_view)> filter)
{
    auto ps = partition_slice_builder(*schema).build();
    query::read_command cmd(schema->id(), schema->version(), ps, std::numeric_limits<uint32_t>::max());
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(key)));
    auto partition_range = dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*schema, std::move(pkey)));
    dht::partition_range_vector partition_ranges;
    partition_ranges.emplace_back(std::move(partition_range));
    return proxy.query(schema, make_lw_shared(std::move(cmd)), std::move(partition_ranges), cl, {timeout, nullptr}).then([ps, schema, filter] (auto qr) {
        return query::result_view::do_with(*qr.query_result, [&, filter] (query::result_view v) {
            auto pd = std::make_shared<prefetched_struct<bytes>>(schema);
            v.consume(ps, prefetched_struct_builder<bytes>(pd, schema, ps, filter));
            return pd;
        });
    });
}

future<std::shared_ptr<prefetched_struct<bytes>>> prefetch_simple(service::storage_proxy& proxy,
   const schema_ptr schema,
   const bytes& key,
   db::consistency_level cl,
   db::timeout_clock::time_point timeout,
   service::client_state& cs)
{
    return prefetch_struct_impl<bytes>(proxy, schema, key, cl, timeout, cs, [] (const column_definition& col, prefetched_struct<bytes>& data, bytes_view cell_view) {
        auto&& dv = col.type->deserialize_value(cell_view);
        auto&& d = dv.serialize();
        data._data = std::move(d);
        data._origin_size = data._data.size();
        return true;
    });
}

class prefetched_multi_struct_builder {
    using data_type = prefetched_struct<std::vector<std::pair<bytes, bytes>>>;
    data_type& _data; 
    const query::partition_slice& _partition_slice;
    const schema_ptr _schema;
    bytes _current;
public:
    prefetched_multi_struct_builder(std::shared_ptr<data_type> data, const schema_ptr schema, const query::partition_slice& ps)
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
future<std::shared_ptr<prefetched_struct<std::vector<std::pair<bytes, bytes>>>>> prefetch_simple(service::storage_proxy& proxy,
    const schema_ptr schema,
    const std::vector<bytes>& keys,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    auto full_slice = partition_slice_builder(*schema).build();
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(),
        full_slice, std::numeric_limits<int32_t>::max(), gc_clock::now(), tracing::make_trace_info(cs.get_trace_state()), query::max_partitions, utils::UUID(), cs.get_timestamp());
    auto partition_ranges = boost::copy_range<dht::partition_range_vector>(keys | boost::adaptors::transformed([schema] (auto& key) {
            auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(key)));
            return dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*schema, std::move(pkey)));
    }));
    // consume the result, and convert it to redis format.
    return proxy.query(schema, command, std::move(partition_ranges), cl, {timeout, nullptr}).then([schema, full_slice] (auto qr) {
        return query::result_view::do_with(*qr.query_result, [&] (query::result_view v) {
            auto pd = std::make_shared<prefetched_struct<std::vector<std::pair<bytes, bytes>>>>(schema);
            v.consume(full_slice, prefetched_multi_struct_builder(pd, schema, full_slice));
            return pd;
        });
    });
}

future<std::shared_ptr<prefetched_list>> prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    long start,
    long end)
{
    return prefetch_struct_impl<std::vector<std::pair<bytes, bytes>>>(proxy, schema, key, cl, timeout, cs, [start, iend = end] (const column_definition& col, prefetched_list& data, bytes_view cell_view) { 
        auto ctype = static_pointer_cast<const collection_type_impl>(col.type);
        auto map_type = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
        auto v = map_type->deserialize(cell_view);
        auto&& n = value_cast<map_type_impl::native_type>(v);
        auto end = iend + static_cast<long>(n.size()); // safe.
        if (start < 0 || end < 0 || start > end ) {
           throw std::logic_error(sprint("cannot prefetch list, start: %ld, end: %ld", start, iend));
        }
        end = end % static_cast<long>(n.size());
        for (size_t i = static_cast<size_t>(start); i <= static_cast<size_t>(end); ++i) {
            auto&& el = n[i];
            data._data.emplace_back(std::move(std::make_pair(el.first.serialize(), el.second.serialize())));
        }
        data._origin_size = n.size();
        data.set_has_more(n.size() - data._data.size() > 0);
        return true;
    });
}

future<std::shared_ptr<prefetched_list>> prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    bool left)
{
    return prefetch_struct_impl<std::vector<std::pair<bytes, bytes>>>(proxy, schema, key, cl, timeout, cs, [left] (const column_definition& col, prefetched_list& data, bytes_view cell_view) { 
        auto ctype = static_pointer_cast<const collection_type_impl>(col.type);
        auto map_type = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
        auto v = map_type->deserialize(cell_view);
        auto&& n = value_cast<map_type_impl::native_type>(v);
        data._origin_size = n.size();
        auto& el = left ? n.front() : n.back();
        data._data.emplace_back(std::move(std::make_pair(el.first.serialize(), el.second.serialize())));
        data.set_has_more(n.size() - data._data.size() > 0);
        return true;
    });
}
future<std::shared_ptr<prefetched_list>> prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    only_size_tag)
{
    return prefetch_struct_impl<std::vector<std::pair<bytes, bytes>>>(proxy, schema, key, cl, timeout, cs, [] (const column_definition& col, prefetched_list& data, bytes_view cell_view) { 
        auto ctype = static_pointer_cast<const collection_type_impl>(col.type);
        auto map_type = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
        auto v = map_type->deserialize(cell_view);
        auto&& n = value_cast<map_type_impl::native_type>(v);
        data._origin_size = n.size();
        data.set_has_more(n.size() - data._data.size() > 0);
        return true;
    });
}
future<std::shared_ptr<prefetched_list>> prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    long index)
{
    return prefetch_struct_impl<std::vector<std::pair<bytes, bytes>>>(proxy, schema, key, cl, timeout, cs, [index] (const column_definition& col, prefetched_list& data, bytes_view cell_view) { 
        auto ctype = static_pointer_cast<const collection_type_impl>(col.type);
        auto map_type = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
        auto v = map_type->deserialize(cell_view);
        auto&& n = value_cast<map_type_impl::native_type>(v);
        data._origin_size = n.size();
        auto l = index + static_cast<long>(n.size());
        if (l < 0) {
            throw std::logic_error(sprint("cannot prefetch index: %ld from list", l));
        }
        l = l % static_cast<long>(n.size());
        auto& el = n[static_cast<size_t>(l)];
        data._data.emplace_back(std::move(std::make_pair(el.first.serialize(), el.second.serialize())));
        data.set_has_more(n.size() - data._data.size() > 0);
        return true;
    });
}

future<std::shared_ptr<prefetched_list>> prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    bytes&& target,
    long count)
{
    return prefetch_struct_impl<std::vector<std::pair<bytes, bytes>>>(proxy, schema, key, cl, timeout, cs, [target, count] (const column_definition& col, prefetched_list& data, bytes_view cell_view) { 
        auto ctype = static_pointer_cast<const collection_type_impl>(col.type);
        auto map_type = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
        auto v = map_type->deserialize(cell_view);
        auto&& n = value_cast<map_type_impl::native_type>(v);
        data._origin_size = n.size();
        size_t c = 0, i = 0;
        if (count < 0) {
            c = 0 - static_cast<size_t>(count);
            for (auto&& el : n | boost::adaptors::reversed) {
                if (i >= c) {
                    break;
                }
                auto&& v = el.second.serialize();
                if (v == target && i < c) {
                    ++i;
                    data._data.emplace_back(std::move(std::make_pair(el.first.serialize(), el.second.serialize())));
                }
            }
        } else {
            c = static_cast<size_t>(count) - 1;
            for (auto&& el : n) {
                if (c >= 0 && i > c) break;
                auto&& v = el.second.serialize();
                if (v == target) {
                    ++i;
                    data._data.emplace_back(std::move(std::make_pair(el.first.serialize(), el.second.serialize())));
                }
            }
        }
        data.set_has_more(n.size() - data._data.size() > 0);
        return true;
    });
}

future<std::shared_ptr<prefetched_map>> prefetch_map(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    const fetch_map_options option,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    return prefetch_struct_impl<std::unordered_map<std::optional<bytes>, std::optional<bytes>>>(proxy, schema, key, cl, timeout, cs, [option] (const column_definition& col, prefetched_map& data, bytes_view cell_view) { 
        auto ctype = static_pointer_cast<const collection_type_impl>(col.type);
        auto map_type = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
        auto v = map_type->deserialize(cell_view);
        auto&& n = value_cast<map_type_impl::native_type>(v);
        data._origin_size = n.size();
        if (option == fetch_map_options::keys) {
            for (auto&& el : n) {
                data._data.emplace(std::move(std::make_pair(std::optional<bytes>(el.first.serialize()), std::optional<bytes>())));
            }
        } else if (option == fetch_map_options::values) {
            for (auto&& el : n) {
                // only fetch the values.
                data._data.emplace(std::move(std::make_pair(std::optional<bytes>(el.second.serialize()), std::optional<bytes>())));
            }
        } else {
            for (auto&& el : n) {
                data._data.emplace(std::move(std::make_pair(std::optional<bytes>(el.first.serialize()), std::optional<bytes>(el.second.serialize()))));
            }
        }
        data.set_has_more(n.size() - data._data.size() > 0);
        return true;
    });
}

future<std::shared_ptr<prefetched_map_only_values>> prefetch_map(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    const std::vector<bytes>& map_keys,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    return prefetch_struct_impl<std::vector<std::optional<bytes>>>(proxy, schema, key, cl, timeout, cs, [&map_keys] (const column_definition& col, prefetched_map_only_values& data, bytes_view cell_view) { 
        auto ctype = static_pointer_cast<const collection_type_impl>(col.type);
        auto map_type = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
        auto v = map_type->deserialize(cell_view);
        auto&& n = value_cast<map_type_impl::native_type>(v);
        data._origin_size = n.size();
        auto&& kvs = boost::copy_range<std::unordered_map<bytes, std::optional<bytes>>>(map_keys | boost::adaptors::transformed([] (const auto& mkey) {
            return std::pair<bytes, std::optional<bytes>>(mkey, std::optional<bytes>());
        }));
        size_t total = kvs.size();
        for (auto&& el : n) {
            auto&& key = el.first.serialize();
            if (kvs.count(key) > 0) {
                auto&& val = el.second.serialize();
                kvs [key] = std::move(std::optional<bytes>(std::move(val)));
                if (--total == 0) {
                    break;
                }
            }
        }
        auto&& values = boost::copy_range<std::vector<std::optional<bytes>>>(map_keys | boost::adaptors::transformed([&kvs] (auto& mkey) {
            return std::move(kvs[mkey]); 
        }));
        data._data = std::move(values);
        data.set_has_more(n.size() - data._data.size() > 0);
        return true;
    });
}

future<std::shared_ptr<prefetched_map_only_one_cell>> prefetch_map(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    const bytes& map_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    return prefetch_struct_impl<std::optional<std::pair<bytes, bytes>>>(proxy, schema, key, cl, timeout, cs, [&map_key] (const column_definition& col, prefetched_map_only_one_cell& data, bytes_view cell_view) { 
        auto ctype = static_pointer_cast<const collection_type_impl>(col.type);
        auto map_type = map_type_impl::get_instance(ctype->name_comparator(), ctype->value_comparator(), true);
        auto v = map_type->deserialize(cell_view);
        auto&& n = value_cast<map_type_impl::native_type>(v);
        data._origin_size = n.size();
        data._data = std::optional<std::pair<bytes, bytes>>();
        for (auto&& el : n) {
            auto&& key = el.first.serialize();
            if (key == map_key) {
                data.set_has_more(n.size() - 1 > 0);
                data._data = std::move(std::optional<std::pair<bytes, bytes>>(std::make_pair(std::move(key), std::move(el.second.serialize()))));
                return true;
            }
        }
        return false;
    });
}
future<bool> exists(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs)
{
    auto full_slice = partition_slice_builder(*schema).build();
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(key)));
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

} // end of redis namespace
