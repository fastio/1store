#include "redis/commands/spop.hh"
#include "redis/commands/unexpected.hh"
#include "seastar/core/shared_ptr.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "db/system_keyspace.hh"
#include "types.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "mutation.hh"
#include "timeout_config.hh"
#include "redis/redis_mutation.hh"
#include "redis/prefetcher.hh"
#include <random>
namespace redis {

namespace commands {

shared_ptr<abstract_command> spop::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count != 1) {
        return unexpected::make_exception(std::move(req._command), sprint("-wrong number of arguments (given %ld, expected 1)\r\n", req._args_count));
    }
    return seastar::make_shared<spop> (std::move(req._command), sets_schema(proxy, cs.get_keyspace()), std::move(req._args[0]));
}

future<redis_message> spop::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_set(proxy, _schema, _key, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        if (pd && pd->has_data()) {
            auto make_random_index = [] (int max) {
                auto gen = std::default_random_engine(std::random_device()());
                auto dist = std::uniform_int_distribution(0, max);
                // std::uniform_int_distribution(0, max) will return [0, max]
                return dist(gen) % max;
            };
            auto index = make_random_index(static_cast<int>(pd->data().size()));
            auto member = *(pd->data()[index].first);
            return redis::write_mutation(proxy, redis::make_set_dead_cells(_schema, _key, std::vector<bytes> { member }), cl, timeout, cs).then_wrapped([this, index, pd] (auto fut) {
                return redis_message::make_set_bytes(pd, index); 
            });
        }
        return redis_message::null(); 
    });
}

}
}
