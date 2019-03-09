#include "redis/commands/srem.hh"
#include "redis/commands/unexpected.hh"
#include "seastar/core/shared_ptr.hh"
#include "redis/reply_builder.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "db/system_keyspace.hh"
#include "types.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "mutation.hh"
#include "timeout_config.hh"
#include "redis/redis_mutation.hh"
//#include "log.hh"
namespace redis {

namespace commands {

shared_ptr<abstract_command> srem::prepare(service::storage_proxy& proxy, request&& req)
{
    if (req._args_count < 2 ) {
        return unexpected::prepare(std::move(req._command), std::move(bytes {msg_syntax_err}));
    }
    std::vector<bytes> data;
    for (size_t i = 1; i < req._args_count; i++) {
        data.emplace_back(std::move(req._args[i]));
    }
    return seastar::make_shared<srem> (std::move(req._command), sets_schema(proxy), std::move(req._args[0]), std::move(data));
}

future<reply> srem::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    auto total = _data.size();
    return redis::write_mutation(proxy, redis::make_set_dead_cells(_schema, _key, std::move(_data)), cl, timeout, cs).then_wrapped([this, total] (auto f) {
        try {
            f.get();
        } catch (std::exception& e) {
            return reply_builder::build<error_tag>();
        }
        return reply_builder::build<number_tag>(total);
    });
}

}
}
