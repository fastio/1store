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

static inline decltype(auto) keyspace() { return redis::NAME; }
static inline decltype(auto) simple_objects() { return redis::SIMPLE_OBJECTS; }
static inline decltype(auto) lists() { return redis::LISTS; }
static inline decltype(auto) sets() { return redis::SETS; }
static inline decltype(auto) maps() { return redis::MAPS; }
static inline decltype(auto) zsets() { return redis::ZSETS; }
static inline const schema_ptr simple_objects_schema(service::storage_proxy& proxy) {
    auto& db = proxy.get_db().local();
    auto schema = db.find_schema(keyspace(), simple_objects());
    return schema;
}
static inline const schema_ptr lists_schema(service::storage_proxy& proxy) {
    auto& db = proxy.get_db().local();
    auto schema = db.find_schema(keyspace(), lists());
    return schema;
}
static inline const schema_ptr sets_schema(service::storage_proxy& proxy) {
    auto& db = proxy.get_db().local();
    auto schema = db.find_schema(keyspace(), sets());
    return schema;
}
static inline const schema_ptr maps_schema(service::storage_proxy& proxy) {
    auto& db = proxy.get_db().local();
    auto schema = db.find_schema(keyspace(), maps());
    return schema;
}
static inline const schema_ptr zsets_schema(service::storage_proxy& proxy) {
    auto& db = proxy.get_db().local();
    auto schema = db.find_schema(keyspace(), zsets());
    return schema;
}

inline long bytes2long(const bytes& b) {
    try {
        return std::atol(make_sstring(b).data());
    } catch (std::exception const & e) {
        throw e;
    }
}
inline bytes long2bytes(long l) {
    auto s = sprint("%lld", l);
    return to_bytes(s);
}
inline double bytes2double(const bytes& b) {
    try {
        return std::atof(make_sstring(b).data());
    } catch (std::exception const & e) {
        throw e;
    }
}
inline bytes double2bytes(double d) {
    auto s = sprint("%lf", d);
    return to_bytes(s);
}
inline bool is_number(const bytes& b)
{
    return !b.empty() && std::find_if(b.begin(), b.end(), [] (auto c) { return !std::isdigit((char)c); }) == b.end();
}

class abstract_command : public enable_shared_from_this<abstract_command> {
protected:
    bytes _name;
    // ttl in millis seconds
    gc_clock::duration _ttl;
    const api::timestamp_type _timestamp;
    const gc_clock::time_point _local_deletion_time;
public:
    abstract_command(bytes&& name, const gc_clock::duration ttl)
        : _name(std::move(name))
        , _ttl(ttl)
        , _timestamp(api::new_timestamp())
        , _local_deletion_time(gc_clock::now())
    {
    }
    abstract_command(bytes&& name)
        : _name(std::move(name))
        , _timestamp(api::new_timestamp())
        , _local_deletion_time(gc_clock::now())
    {
    }
    virtual ~abstract_command() {};
    
    virtual future<reply> execute(service::storage_proxy&, db::consistency_level cl, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& client_state) = 0;
    const bytes& name() const { return _name; }
    static inline sstring make_sstring(const bytes& b) {
        return sstring{reinterpret_cast<const char*>(b.data()), b.size()};
    }
};

} // end of redis namespace
