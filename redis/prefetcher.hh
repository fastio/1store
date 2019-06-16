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
enum class fetch_options {
    all,
    keys,
    values,
    simple,
};

future<map_return_type> prefetch_set(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs
    );
future<map_return_type> prefetch_list(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    const fetch_options option,
    bool reversed,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs
    );
future<map_return_type> prefetch_map(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    const std::vector<bytes> ckeys,
    fetch_options option,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs
    );
future<map_return_type> prefetch_map(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    fetch_options option,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs
    );
future<zset_return_type> prefetch_zset(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    const std::vector<bytes> ckeys,
    fetch_options option,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs
    );
future<zset_return_type> prefetch_zset(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    fetch_options option,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs
    );
future<bytes_return_type> prefetch_simple(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs
    );
future<mbytes_return_type> prefetch_simple(service::storage_proxy& proxy,
    const schema_ptr schema,
    const std::vector<bytes>& keys,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs
    );
future<bool> exists(service::storage_proxy& proxy,
    const schema_ptr schema,
    const bytes& key,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& cs
    );
} // end of redis namespace
