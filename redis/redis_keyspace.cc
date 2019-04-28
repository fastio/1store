#include "redis/redis_keyspace.hh"
#include "service/migration_manager.hh"
#include "schema_builder.hh"
#include "types.hh"
#include "exceptions/exceptions.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include "seastar/core/future.hh"
#include <memory>
#include "log.hh"
#include "db/query_context.hh"
#include "auth/service.hh"
#include "service/client_state.hh"
#include "transport/server.hh"
#include "db/system_keyspace.hh"
#include "schema.hh"
using namespace seastar;
namespace redis {

static logging::logger log("redis_keyspace");
schema_ptr strings_schema(sstring ks_name) {
     schema_builder builder(make_lw_shared(schema(generate_legacy_id(ks_name, redis::STRINGS), ks_name, redis::STRINGS,
     // partition key
     {{"pkey", utf8_type}},
     // clustering key
     {},
     // regular columns
     {{"data", utf8_type}},
     // static columns
     {},
     // regular column name type
     utf8_type,
     // comment
     "save strings for redis"
    )));
    builder.set_gc_grace_seconds(0);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr lists_schema(sstring ks_name) {
     schema_builder builder(make_lw_shared(schema(generate_legacy_id(ks_name, redis::LISTS), ks_name, redis::LISTS,
     // partition key
     {{"pkey", utf8_type}},
     // clustering key
     {{"ckey", bytes_type}},
     // regular columns
     {{"data", utf8_type}},
     // static columns
     {},
     // regular column name type
     utf8_type,
     // comment
     "save lists for redis"
    )));
    builder.set_gc_grace_seconds(0);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr maps_schema(sstring ks_name) {
     schema_builder builder(make_lw_shared(schema(generate_legacy_id(ks_name, redis::MAPS), ks_name, redis::MAPS,
     // partition key
     {{"pkey", utf8_type}},
     // clustering key
     {{"ckey", utf8_type}},
     // regular columns
     {{"data", utf8_type}},
     // static columns
     {},
     // regular column name type
     utf8_type,
     // comment
     "save maps for redis"
    )));
    builder.set_gc_grace_seconds(0);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr sets_schema(sstring ks_name) {
     schema_builder builder(make_lw_shared(schema(generate_legacy_id(ks_name, redis::SETS), ks_name, redis::SETS,
     // partition key
     {{"pkey", utf8_type}},
     // clustering key
     {{"ckey", utf8_type}},
     // regular columns
     {{"data", boolean_type}},
     // static columns
     {},
     // regular column name type
     utf8_type,
     // comment
     "save sets for redis"
    )));
    builder.set_gc_grace_seconds(0);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr zsets_schema(sstring ks_name) {
     schema_builder builder(make_lw_shared(schema(generate_legacy_id(ks_name, redis::ZSETS), ks_name, redis::ZSETS,
     // partition key
     {{"pkey", utf8_type}},
     // clustering key
     {{"ckey", utf8_type}},
     // regular columns
     {{"data", utf8_type}},
     // static columns
     {},
     // regular column name type
     utf8_type,
     // comment
     "save sorted sets for redis"
    )));
    builder.set_gc_grace_seconds(0);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}
future<> redis_keyspace_helper::create_if_not_exists(lw_shared_ptr<db::config> config) {
    auto keyspace_replication_properties = config->redis_keyspace_replication_properties();
    if (keyspace_replication_properties.count("class") == 0) {
        keyspace_replication_properties["class"] = "SimpleStrategy";
        keyspace_replication_properties["replication_factor"] = "1";
    }
    auto keyspace_gen = [config, keyspace_replication_properties = std::move(keyspace_replication_properties)]  (sstring name) {
        auto& proxy = service::get_local_storage_proxy();
        if (proxy.get_db().local().has_keyspace(name)) {
            return make_ready_future<>();
        }
        auto attrs = make_shared<cql3::statements::ks_prop_defs>();
        attrs->add_property(cql3::statements::ks_prop_defs::KW_DURABLE_WRITES, "true");
        std::map<sstring, sstring> replication_properties;
        for (auto&& e : keyspace_replication_properties) {
            replication_properties.emplace(e.first, e.second);
        }
        attrs->add_property(cql3::statements::ks_prop_defs::KW_REPLICATION, replication_properties); 
        attrs->validate();
        return service::get_local_migration_manager().announce_new_keyspace(attrs->as_ks_metadata(name), false);
    };
    auto table_gen = [] (sstring ks_name, sstring cf_name, schema_ptr schema) {
        auto& proxy = service::get_local_storage_proxy();
        if (proxy.get_db().local().has_schema(ks_name, cf_name)) {
            return make_ready_future<>();
        }
        return service::get_local_migration_manager().announce_new_column_family(schema, false);
    };
    // create 16 default database for redis.
    return parallel_for_each(boost::irange<unsigned>(0, 16), [keyspace_gen = std::move(keyspace_gen), table_gen = std::move(table_gen)] (auto c) {
        auto ks_name = sprint("redis_%d", c);
        return keyspace_gen(ks_name).then([ks_name, table_gen] {
            return when_all_succeed(
                table_gen(ks_name, redis::STRINGS, strings_schema(ks_name)),
                table_gen(ks_name, redis::LISTS, lists_schema(ks_name)),
                table_gen(ks_name, redis::SETS, sets_schema(ks_name)),
                table_gen(ks_name, redis::MAPS, maps_schema(ks_name)),
                table_gen(ks_name, redis::ZSETS, zsets_schema(ks_name))
            ).then([] {
                return make_ready_future<>();
            });
        });
    });
}
}
