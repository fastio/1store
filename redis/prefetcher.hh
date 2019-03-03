#pragma once
#include "bytes.hh"
#include "seastar/core/future.hh"
#include "seastar/core/sstring.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "mutation.hh"
#include "db/consistency_level_type.hh"
#include "db/timeout_clock.hh"
#include "db/system_keyspace.hh"
#include "service/storage_proxy.hh"
#include "keys.hh"
#include "timestamp.hh"
#include "redis/redis_keyspace.hh"
#include <unordered_map>
using namespace seastar;

class timeout_config;

namespace service {
//class storage_proxy;
class client_state;
}

namespace cql3 {
class query_options;
}

namespace tracing {
class trace_state_ptr;
}

namespace redis {

// Read required partition for write-before-read operations.
template<typename ContainerType>
struct prefetched_struct {
    const schema_ptr _schema;
    bool _inited = false;
    bool _has_more = false;
    size_t _origin_size = 0;
    ContainerType _data;
    prefetched_struct(const schema_ptr schema) : _schema(schema) {}
    bool has_data() const { return _inited; }
    bool has_more() const { return _has_more; }
    void set_has_more(bool v) { _has_more = v; }
    size_t data_size() const { return _data.size(); }
    size_t origin_size() const { return _origin_size; }
    ContainerType& data() { return _data; }
    const ContainerType& data() const { return _data; }
};

using prefetched_map = prefetched_struct<std::unordered_map<bytes, bytes>>;
future<std::shared_ptr<prefetched_map>> prefetch_map(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs
    );
using prefetched_map_only_values = prefetched_struct<std::vector<std::optional<bytes>>>;
future<std::shared_ptr<prefetched_map_only_values>> prefetch_map(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    const std::vector<bytes>& map_keys,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs
    );
using prefetched_map_only_one_cell = prefetched_struct<std::optional<std::pair<bytes, bytes>>>;
future<std::shared_ptr<prefetched_map_only_one_cell>> prefetch_map(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    const bytes& map_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs
    );
using prefetched_simple = prefetched_struct<bytes>;
future<std::shared_ptr<prefetched_simple>> prefetch_simple(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs
    );
future<std::shared_ptr<prefetched_struct<std::vector<std::pair<bytes, bytes>>>>> prefetch_simple(service::storage_proxy& proxy,
    const schema_ptr schema,
    const std::vector<bytes>& keys,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs
    );
future<bool> exists(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& raw_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs
    );
using prefetched_list = prefetched_struct<std::vector<std::pair<bytes, bytes>>>;
future<std::shared_ptr<prefetched_list>> prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& raw_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    bool left
    );
future<std::shared_ptr<prefetched_list>> prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& raw_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    long start,
    long end
    );
struct only_size_tag {};
future<std::shared_ptr<prefetched_list>> prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& raw_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    only_size_tag
    );
future<std::shared_ptr<prefetched_list>> prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& raw_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    long index 
    );
future<std::shared_ptr<prefetched_list>> prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& raw_key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs,
    bytes&& target,
    long count 
    );
} // end of redis namespace
