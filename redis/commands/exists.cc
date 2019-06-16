#include "redis/commands/exists.hh"
#include "redis/commands/unexpected.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "timeout_config.hh"
#include "service/client_state.hh"
#include "service/storage_proxy.hh"
#include "db/system_keyspace.hh"
#include "partition_slice_builder.hh"
#include "gc_clock.hh"
#include "dht/i_partitioner.hh"
#include "redis/prefetcher.hh"
namespace redis {
namespace commands {
shared_ptr<abstract_command> exists::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 1) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 1, req._args_count);
    }
    std::vector<schema_ptr> schemas {
        simple_objects_schema(proxy, cs.get_keyspace()),
        lists_schema(proxy, cs.get_keyspace()),
        sets_schema(proxy, cs.get_keyspace()),
        maps_schema(proxy, cs.get_keyspace()),
        zsets_schema(proxy, cs.get_keyspace())
    };
    return seastar::make_shared<exists> (std::move(req._command), std::move(schemas), std::move(req._args[0]));
}

future<redis_message> exists::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    auto check_exists = [this, timeout, &proxy, cl, &tc, &cs] (const schema_ptr schema) {
        return redis::exists(proxy, schema, _key, cl, timeout, cs);
    };
    auto mapper = make_lw_shared<decltype(check_exists)>(std::move(check_exists));
    return map_reduce(_schemas.begin(), _schemas.end(), *mapper, false, std::bit_or<bool> ()).then([mapper = std::move(mapper)] (auto result) {
        if (result) {
            return redis_message::ok();
        }
        return redis_message::err();
    });
}
}
}
