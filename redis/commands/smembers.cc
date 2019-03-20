#include "redis/commands/smembers.hh"
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
//#include "log.hh"
namespace redis {

namespace commands {

shared_ptr<abstract_command> smembers::prepare(service::storage_proxy& proxy, request&& req)
{
    if (req._args_count < 1 ) {
        return unexpected::prepare(std::move(req._command), std::move(bytes {msg_syntax_err}));
    }
    return seastar::make_shared<smembers> (std::move(req._command), sets_schema(proxy), std::move(req._args[0]));
}

future<redis_message> smembers::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_set(proxy, _schema, _key, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        if (pd && pd->has_data()) {
            auto&& result = boost::copy_range<std::vector<std::optional<bytes>>> (pd->data() | boost::adaptors::transformed([this] (auto& data) {
                return std::move(data.first); 
            }));
            return redis_message::make(std::move(result));
        }
        return redis_message::null(); 
    });
}

}
}
