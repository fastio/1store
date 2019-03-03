#include "redis/commands/hset.hh"
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

//logging::logger log("hset");
shared_ptr<abstract_command> hset::prepare(service::storage_proxy& proxy, request&& req, bool multi)
{
    if (req._args_count < 3 || req._args_count % 2 != 1 || (multi && req._args_count > 3)) {
        return unexpected::prepare(std::move(req._command), std::move(bytes {msg_syntax_err}));
    }
    std::unordered_map<bytes, bytes> data;
    for (size_t i = 1; i < req._args_count; i+= 2) {
        data.emplace(std::make_pair(req._args[i], req._args[i + 1]));
    }
    return seastar::make_shared<hset> (std::move(req._command), maps_schema(proxy), std::move(req._args[0]), std::move(data), multi);
}

future<reply> hset::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    auto total = _data.size();
    return redis::write_mutation(proxy, redis::make_map_cells(_schema, _key, std::move(_data)), cl, timeout, cs).then_wrapped([this, total] (auto f) {
        try {
            f.get();
        } catch (std::exception& e) {
            return reply_builder::build<error_tag>();
        }
        return _multi == false ? reply_builder::build<number_tag>(total) : reply_builder::build<OK_tag>();
    });
}

}
}
