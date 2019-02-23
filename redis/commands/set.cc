#include "redis/commands/set.hh"
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
#include "log.hh"
namespace redis {

namespace commands {

static logging::logger log("command_set");

shared_ptr<abstract_command> set::prepare(service::storage_proxy& proxy, request&& req)
{
    if (req._args_count < 2) {
        return unexpected::prepare(std::move(req._command), std::move(bytes {msg_syntax_err}));
    }
    else if (req._args_count > 2) {
        // FIXME: more other options
    }
    return make_shared<set> (std::move(req._command), simple_objects_schema(proxy), std::move(req._args[0]), std::move(req._args[1]));
}

future<reply> set::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    return write_mutation(proxy, _schema, _key, std::move(_data), cl, timeout, cs).then_wrapped([this] (auto f) {
        try {
            f.get();
        } catch (...) {
            // FIXME: what kind of exceptions.
            return reply_builder::build<error_tag>();
        }
        return reply_builder::build<one_tag>();
    });
}
}
}
