#include "redis/commands/scard.hh"
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
namespace redis {

namespace commands {

shared_ptr<abstract_command> scard::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count != 1) {
        return unexpected::make_exception(std::move(req._command), sprint("-wrong number of arguments (given %ld, expected 1)\r\n", req._args_count));
    }
    return seastar::make_shared<scard> (std::move(req._command), sets_schema(proxy, cs.get_keyspace()), std::move(req._args[0]));
}

future<redis_message> scard::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_set(proxy, _schema, _key, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        if (pd && pd->has_data()) {
            return redis_message::make_long(static_cast<long>(pd->data().size()));
        }
        return redis_message::null(); 
    });
}

}
}
