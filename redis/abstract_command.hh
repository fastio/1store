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
inline bool is_number(const bytes& b)
{
    return !b.empty() && std::find_if(b.begin(), b.end(), [] (auto c) { return !std::isdigit((char)c); }) == b.end();
}

class precision_time {
   public:
       static constexpr db_clock::time_point REFERENCE_TIME{std::chrono::milliseconds(1262304000000)};
   private:
       static thread_local precision_time _last;
   public:
       db_clock::time_point _millis;
       int32_t _nanos;

       static precision_time get_next(db_clock::time_point millis);
};  

struct partition_dead_tag {}; 
struct cell_dead_tag {}; 
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
    /*
    static inline gc_clock::duration make_ttl(long ttl) {
        return db_clock::from_time_t({ 0 }) + std::chrono::milliseconds(ttl * 1000);
    }
    */

    atomic_cell make_dead_cell() const {
        // _timestamp(api::new_timestamp())
        // _local_deletion_time(gc_clock::now())
        return atomic_cell::make_dead(api::new_timestamp(), gc_clock::now());
    }   

    atomic_cell make_cell(const schema_ptr schema, const abstract_type& type, const fragmented_temporary_buffer::view& value, atomic_cell::collection_member cm = atomic_cell::collection_member::no) const {
        auto ttl = _ttl;

        if (ttl.count() <= 0) {
            ttl = schema->default_time_to_live();
        }   

        if (ttl.count() > 0) {
            return atomic_cell::make_live(type, _timestamp, value, _local_deletion_time + ttl, ttl, cm);
        } else {
            return atomic_cell::make_live(type, _timestamp, value, cm);
        }   
    };  

    atomic_cell make_cell(const schema_ptr schema, const abstract_type& type, bytes_view value, atomic_cell::collection_member cm = atomic_cell::collection_member::no) const {
        return make_cell(schema, type, fragmented_temporary_buffer::view(value), cm);
    }   
   
    mutation make_mutation(const schema_ptr schema, const bytes& key) {
        auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(abstract_command::make_sstring(key)));
        return std::move(mutation(schema, std::move(pkey)));
    }
    future<> write_mutation(service::storage_proxy&, const schema_ptr schema, const bytes& key, bytes&& data, db::consistency_level cl, db::timeout_clock::time_point timeout, service::client_state& client_state);
    future<> write_mutation(service::storage_proxy&, const schema_ptr schema, const bytes& key, partition_dead_tag, db::consistency_level cl, db::timeout_clock::time_point timeout, service::client_state& client_state);
    future<> write_mutation(service::storage_proxy&, const schema_ptr schema, std::vector<std::pair<bytes, bytes>>&& datas, db::consistency_level cl, db::timeout_clock::time_point timeout, service::client_state& client_state);
    future<> write_list_mutation(service::storage_proxy& proxy, const schema_ptr schema, const bytes& key, std::vector<bytes>&& data, db::consistency_level cl, db::timeout_clock::time_point timeout, service::client_state& cs, bool left);
    future<> write_list_mutation(service::storage_proxy& proxy, const schema_ptr schema, const bytes& key, std::vector<std::pair<bytes, bytes>>&& data, db::consistency_level cl, db::timeout_clock::time_point timeout, service::client_state& cs);
    future<> write_list_dead_cell_mutation(service::storage_proxy& proxy, const schema_ptr schema, const bytes& key, std::vector<bytes>&& cell_keys, db::consistency_level cl, db::timeout_clock::time_point timeout, service::client_state& cs);
};

} // end of redis namespace
