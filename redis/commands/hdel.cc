#include "redis/commands/hdel.hh"
#include "redis/commands/unexpected.hh"
#include "seastar/core/shared_ptr.hh"
#include "redis/request.hh"
#include "redis/redis_mutation.hh"
#include "redis/reply.hh"
#include "db/system_keyspace.hh"
#include "types.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "mutation.hh"
#include "timeout_config.hh"
#include "redis/prefetcher.hh"
namespace service {
class storage_proxy;
}
namespace redis {

namespace commands {

shared_ptr<abstract_command> hdel::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count != 2) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 2, req._args_count);
    }
    std::vector<bytes> map_keys;
    map_keys.reserve(req._args.size() - 1);
    map_keys.insert(map_keys.end(), std::make_move_iterator(++(req._args.begin())), std::make_move_iterator(req._args.end()));
    return seastar::make_shared<hdel> (std::move(req._command), maps_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), std::move(map_keys));
}

future<redis_message> hdel::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    return prefetch_map(proxy, _schema, _key, _map_keys, fetch_options::keys, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        return [this, &proxy, cl, timeout, &cs, pd] {
            if (pd && pd->has_data()) {
                // remove the partition
                //return redis::write_mutation(proxy, redis::make_dead(_schema, _key), cl, timeout, cs);
                return redis::write_mutation(proxy, redis::make_map_dead_cells(_schema, _key, std::move(_map_keys)), cl, timeout, cs);
            }
            return make_ready_future<>();
        } ().then_wrapped([this] (auto f) {
            try {
                f.get();
            } catch(std::exception& e) {
                return redis_message::err();
            }
            return redis_message::ok();
        });
    });
}
}
}
