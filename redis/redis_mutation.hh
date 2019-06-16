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

using partition_dead_tag = char;  
struct partition_ttl_tag {};
template<typename ContainerType>
struct redis_mutation {
    const schema_ptr _schema;
    bytes _partition_key;
    ContainerType _data;
    long _ttl = 0;
    redis_mutation(const schema_ptr schema, const bytes& key, ContainerType&& data, long ttl = 0)
        : _schema(schema)
        , _partition_key(key)
        , _data(std::move(data))
        , _ttl(ttl)
    {
    }
    ~redis_mutation()
    {
    }
    const schema_ptr schema() const { return _schema; }
    ContainerType& data() { return _data; }
    bytes& key() { return _partition_key; }
    long ttl() { return _ttl; }
};

struct list_cells {
    std::vector<bytes> _cells;
    bool _reversed;
    size_t size() const { return _cells.size(); }
    list_cells(std::vector<bytes>&& cells, bool reversed) : _cells(std::move(cells)), _reversed(reversed) {}
};
using list_mutation = redis_mutation<list_cells>;

struct list_indexed_cells {
    std::vector<std::pair<std::optional<bytes>, std::optional<bytes>>> _indexed_cells;
    size_t size() const { return _indexed_cells.size(); }
    list_indexed_cells(std::vector<std::pair<std::optional<bytes>, std::optional<bytes>>>&& indexed_cells) : _indexed_cells(std::move(indexed_cells)) {}
};
using list_indexed_cells_mutation = redis_mutation<list_indexed_cells>;

struct map_indexed_cells {
    std::vector<std::pair<std::optional<bytes>, std::optional<bytes>>> _indexed_cells;
    size_t size() const { return _indexed_cells.size(); }
    map_indexed_cells(std::vector<std::pair<std::optional<bytes>, std::optional<bytes>>>&& indexed_cells) : _indexed_cells(std::move(indexed_cells)) {}
};
using map_indexed_cells_mutation = redis_mutation<map_indexed_cells>;

struct set_indexed_cells {
    std::vector<std::pair<std::optional<bytes>, std::optional<bytes>>> _indexed_cells;
    size_t size() const { return _indexed_cells.size(); }
    set_indexed_cells(std::vector<std::pair<std::optional<bytes>, std::optional<bytes>>>&& indexed_cells) : _indexed_cells(std::move(indexed_cells)) {}
};
using set_indexed_cells_mutation = redis_mutation<set_indexed_cells>;

struct zset_indexed_cells {
    std::vector<std::pair<std::optional<bytes>, std::optional<double>>> _indexed_cells;
    size_t size() const { return _indexed_cells.size(); }
    zset_indexed_cells(std::vector<std::pair<std::optional<bytes>, std::optional<double>>>&& indexed_cells) : _indexed_cells(std::move(indexed_cells)) {}
};
using zset_indexed_cells_mutation = redis_mutation<zset_indexed_cells>;

struct list_dead_cells {
    std::vector<std::optional<bytes>> _cell_keys;
    size_t size() const { return _cell_keys.size(); }
    list_dead_cells(std::vector<std::optional<bytes>>&& cell_keys) : _cell_keys(std::move(cell_keys)) {}
};

struct set_cells {
    std::vector<bytes> _cells;
    size_t size() const { return _cells.size(); }
    set_cells(std::vector<bytes>&& cells) : _cells(std::move(cells)) {}
};

using set_mutation = redis_mutation<set_cells>;
struct map_dead_cells {
    std::vector<bytes> _map_keys;
    size_t size() const { return _map_keys.size(); }
    map_dead_cells(std::vector<bytes>&& map_keys) : _map_keys(std::move(map_keys)) {}
};
using list_dead_cells_mutation = redis_mutation<list_dead_cells>;

using map_mutation = redis_mutation<std::unordered_map<bytes, bytes>>;

using map_dead_cells_mutation = redis_mutation<map_dead_cells>;

struct set_dead_cells {
    std::vector<bytes> _map_keys;
    size_t size() const { return _map_keys.size(); }
    set_dead_cells(std::vector<bytes>&& map_keys) : _map_keys(std::move(map_keys)) {}
};
using set_dead_cells_mutation = redis_mutation<set_dead_cells>;

struct zset_cells {
    std::vector<std::pair<bytes, bytes>> _cells;
    size_t size() const { return _cells.size(); }
    zset_cells(std::vector<std::pair<bytes, bytes>>&& cells) : _cells(std::move(cells)) {}
};
struct zset_dead_cells {
    std::vector<bytes> _map_keys;
    size_t size() const { return _map_keys.size(); }
    zset_dead_cells(std::vector<bytes>&& map_keys) : _map_keys(std::move(map_keys)) {}
};

using zset_mutation = redis_mutation<zset_cells>;
using zset_dead_cells_mutation = redis_mutation<zset_dead_cells>;

static inline seastar::lw_shared_ptr<redis_mutation<bytes>> make_simple(const schema_ptr schema, const bytes& key, bytes&& data, long ttl = 0) {
    return seastar::make_lw_shared<redis_mutation<bytes>>(schema, key, std::move(data), ttl);
}
static inline seastar::lw_shared_ptr<redis_mutation<partition_dead_tag>> make_dead(const schema_ptr schema, const bytes& key) {
    return seastar::make_lw_shared<redis_mutation<partition_dead_tag>>(schema, key, std::move(partition_dead_tag { 0 }));
}
static inline seastar::lw_shared_ptr<redis_mutation<partition_dead_tag>> make_dead(const schema_ptr schema, const bytes& key, long ttl) {
    return seastar::make_lw_shared<redis_mutation<partition_dead_tag>>(schema, key, std::move(partition_dead_tag { 0 }), ttl);
}
/*
static inline seastar::lw_shared_ptr<redis_mutation<partition_ttl_tag>> make_dead(const schema_ptr schema, const bytes& key, long ttl) {
    return seastar::make_lw_shared<redis_mutation<partition_ttl_tag>>(schema, key, std::move(partition_dead_tag { 0 }));
}
*/
static inline seastar::lw_shared_ptr<list_mutation> make_list_cells(const schema_ptr schema, const bytes& key, std::vector<bytes>&& cells, bool reversed, long ttl = 0) {
    return seastar::make_lw_shared<list_mutation> (schema, key, std::move(list_cells (std::move(cells), reversed)), ttl);
}
static inline seastar::lw_shared_ptr<list_indexed_cells_mutation> make_list_indexed_cells(const schema_ptr schema,
    const bytes& key,
    std::vector<std::pair<std::optional<bytes>, std::optional<bytes>>>&& indexed_cells, long ttl = 0)
{
    return seastar::make_lw_shared<list_indexed_cells_mutation> (schema, key, std::move(list_indexed_cells (std::move(indexed_cells))), ttl);
}
static inline seastar::lw_shared_ptr<list_dead_cells_mutation> make_list_dead_cells(const schema_ptr schema, const bytes& key, std::vector<std::optional<bytes>>&& cell_keys) {
    return seastar::make_lw_shared<list_dead_cells_mutation> (schema, key, std::move(list_dead_cells (std::move(cell_keys))));
}

static inline seastar::lw_shared_ptr<map_mutation> make_map_cells(const schema_ptr schema, const bytes& key, std::unordered_map<bytes, bytes>&& cells, long ttl = 0) {
    return seastar::make_lw_shared<map_mutation> (schema, key, std::move(cells), ttl);
}
static inline seastar::lw_shared_ptr<map_indexed_cells_mutation> make_map_indexed_cells(const schema_ptr schema,
    const bytes& key,
    std::vector<std::pair<std::optional<bytes>, std::optional<bytes>>>&& indexed_cells, long ttl = 0)
{
    return seastar::make_lw_shared<map_indexed_cells_mutation> (schema, key, std::move(map_indexed_cells (std::move(indexed_cells))), ttl);
}
static inline seastar::lw_shared_ptr<map_dead_cells_mutation> make_map_dead_cells(const schema_ptr schema, const bytes& key, std::vector<bytes>&& map_keys) {
    return seastar::make_lw_shared<map_dead_cells_mutation> (schema, key, std::move(map_dead_cells (std::move(map_keys))));
}

static inline seastar::lw_shared_ptr<set_mutation> make_set_cells(const schema_ptr schema, const bytes& key, std::vector<bytes>&& cells, long ttl = 0) {
    return seastar::make_lw_shared<set_mutation> (schema, key, std::move(cells), ttl);
}
static inline seastar::lw_shared_ptr<set_indexed_cells_mutation> make_set_indexed_cells(const schema_ptr schema,
    const bytes& key,
    std::vector<std::pair<std::optional<bytes>, std::optional<bytes>>>&& indexed_cells, long ttl = 0)
{
    return seastar::make_lw_shared<set_indexed_cells_mutation> (schema, key, std::move(set_indexed_cells (std::move(indexed_cells))), ttl);
}
static inline seastar::lw_shared_ptr<set_dead_cells_mutation> make_set_dead_cells(const schema_ptr schema, const bytes& key, std::vector<bytes>&& set_keys) {
    return seastar::make_lw_shared<set_dead_cells_mutation> (schema, key, std::move(set_dead_cells (std::move(set_keys))));
}

static inline seastar::lw_shared_ptr<zset_mutation> make_zset_cells(const schema_ptr schema, const bytes& key, std::vector<std::pair<bytes, bytes>>&& cells, long ttl = 0) {
    return seastar::make_lw_shared<zset_mutation> (schema, key, std::move(cells), ttl);
}
static inline seastar::lw_shared_ptr<zset_indexed_cells_mutation> make_zset_indexed_cells(const schema_ptr schema,
    const bytes& key,
    std::vector<std::pair<std::optional<bytes>, std::optional<double>>>&& indexed_cells, long ttl = 0)
{
    return seastar::make_lw_shared<zset_indexed_cells_mutation> (schema, key, std::move(zset_indexed_cells (std::move(indexed_cells))), ttl);
}
static inline seastar::lw_shared_ptr<zset_dead_cells_mutation> make_zset_dead_cells(const schema_ptr schema, const bytes& key, std::vector<bytes>&& set_keys) {
    return seastar::make_lw_shared<zset_dead_cells_mutation> (schema, key, std::move(zset_dead_cells (std::move(set_keys))));
}

namespace internal {
mutation make_mutation(seastar::lw_shared_ptr<redis_mutation<bytes>> r);
mutation make_mutation(seastar::lw_shared_ptr<redis_mutation<partition_dead_tag>> r);
mutation make_mutation(seastar::lw_shared_ptr<list_mutation> r);
mutation make_mutation(seastar::lw_shared_ptr<list_indexed_cells_mutation> r);
mutation make_mutation(seastar::lw_shared_ptr<list_dead_cells_mutation> r);
mutation make_mutation(seastar::lw_shared_ptr<map_mutation> r);
mutation make_mutation(seastar::lw_shared_ptr<map_indexed_cells_mutation> r);
mutation make_mutation(seastar::lw_shared_ptr<map_dead_cells_mutation> r);
mutation make_mutation(seastar::lw_shared_ptr<set_mutation> r);
mutation make_mutation(seastar::lw_shared_ptr<set_indexed_cells_mutation> r);
mutation make_mutation(seastar::lw_shared_ptr<set_dead_cells_mutation> r);
mutation make_mutation(seastar::lw_shared_ptr<zset_mutation> r);
mutation make_mutation(seastar::lw_shared_ptr<zset_indexed_cells_mutation> r);
mutation make_mutation(seastar::lw_shared_ptr<zset_dead_cells_mutation> r);
future<> write_mutation_impl(
    service::storage_proxy&,
    std::vector<mutation>&& ms,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& client_state
);
}

template<typename ContainerType>
future<> write_mutation(
    service::storage_proxy& proxy,
    seastar::lw_shared_ptr<redis_mutation<ContainerType>> r,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& client_state
)
{
    return internal::write_mutation_impl(proxy, std::vector<mutation> { std::move(internal::make_mutation(r)) }, cl ,timeout, client_state).finally([r] {});
}

future<> write_mutations(
    service::storage_proxy& proxy,
    std::vector<seastar::lw_shared_ptr<redis_mutation<bytes>>> ms,
    db::consistency_level cl,
    db::timeout_clock::time_point timeout,
    service::client_state& client_state
);

} // end of redis namespace
