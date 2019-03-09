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
using namespace seastar;
namespace redis {

static logging::logger log("redis_keyspace");

future<> redis_keyspace_helper::create_if_not_exists(lw_shared_ptr<db::config> config) {
    // FIXME: read the properties from config.
    auto create_keyspace = sprint("create keyspace if not exists %s with replication = {'class' : '%s', 'replication_factor' : %d }",
        redis::NAME, "SimpleStrategy", 1);
    return db::execute_cql(create_keyspace).then_wrapped([] (auto f) {
        try {
            f.get();
        } catch(std::exception& e) {
            throw e;
        }
        return when_all(
            db::execute_cql(sprint("create table if not exists %s.%s (pkey text primary key, data text)", redis::NAME, redis::SIMPLE_OBJECTS)).discard_result(),
            db::execute_cql(sprint("create table if not exists %s.%s (pkey text, ckey blob, data text, primary key(pkey, ckey))", redis::NAME, redis::LISTS)).discard_result(),
            db::execute_cql(sprint("create table if not exists %s.%s (pkey text, ckey text, data boolean, primary key(pkey, ckey))", redis::NAME, redis::SETS)).discard_result(),
            db::execute_cql(sprint("create table if not exists %s.%s (pkey text, ckey text, data text, primary key(pkey, ckey))", redis::NAME, redis::MAPS)).discard_result()
        ).then_wrapped([] (auto f) {
            try {
                f.get();
            } catch(std::exception& e) {
                throw e;
            }
            return make_ready_future<>();
        });
    });
}
}
