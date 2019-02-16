#pragma once
#include "bytes.hh"
#include "seastar/core/future.hh"
#include "seastar/core/sstring.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "mutation.hh"
#include "db/consistency_level_type.hh"
#include "db/timeout_clock.hh"
#include "keys.hh"
using namespace seastar;

class timeout_config;

namespace service {
class storage_proxy;
class client_state;
}

namespace cql3 {
class query_options;
}

namespace tracing {
class trace_state_ptr;
}

namespace redis {

bytes keyspace();

class abstract_command : public enable_shared_from_this<abstract_command> {
protected:
    bytes _name;
public:
    abstract_command(bytes&& name) : _name(std::move(name)) {}
    virtual ~abstract_command() {};
    virtual future<reply> execute(service::storage_proxy&, db::consistency_level cl, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& client_state) = 0;
    const bytes& name() const { return _name; }
    static sstring make_sstring(bytes b) {
        return sstring{reinterpret_cast<char*>(b.data()), b.size()};
    }
};

class mutation_helper final {
public:
    static mutation make_mutation(const schema_ptr schema, const bytes& key) {
        auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(abstract_command::make_sstring(key)));
        return std::move(mutation(schema, std::move(pkey)));
    }
    static future<> write_mutation(service::storage_proxy&, const schema_ptr schema, const bytes& key, bytes&& data, db::consistency_level cl, db::timeout_clock::time_point timeout, service::client_state& client_state);
};

// Read required partition for write-before-read operations.
struct prefetched_partition_collection {
    //const gc_clock::duration _ttl;
    //const gc_clock::time_point _local_deletion_time;
    //const api::timestamp_type _timestamp;
    //partition_key _key;
    //const query_options& _options;
    const schema_ptr _schema;
    bool _inited = false;
    struct cell {
        bytes _key;
        bytes _value;
    };
    using cell_list = std::vector<cell>;
    using row = std::unordered_map<bytes, cell_list>;
    row _row;
    prefetched_partition_collection(const schema_ptr schema) : _schema(schema) {}
    row& partition() { return _row; }
};

struct prefetched_partition_simple {
    const schema_ptr _schema;
    bytes _data;
    bool _inited;
    prefetched_partition_simple(const schema_ptr schema, bytes&& b) : _schema(schema), _data(std::move(b)), _inited(true) {}
    prefetched_partition_simple(const schema_ptr schema) : _schema(schema), _inited(false) {}
    const bool& fetched() const { return _inited; }
};

class prefetch_partition_helper final {
public:
    static future<std::unique_ptr<prefetched_partition_simple>> prefetch_simple(service::storage_proxy& proxy,
        const schema_ptr schema,
        const bytes& raw_key,
        db::consistency_level cl,
        db::timeout_clock::time_point timeout,
        service::client_state& cs);
    static future<std::unique_ptr<prefetched_partition_collection>> prefetch_collection(service::storage_proxy& proxy,
        const schema_ptr schema,
        const bytes& raw_key,
        db::consistency_level cl,
        db::timeout_clock::time_point timeout,
        service::client_state& cs);
};
} // end of redis namespace
