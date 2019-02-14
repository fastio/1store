#include "redis/commands/del.hh"
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

static logging::logger log("command_del");

shared_ptr<abstract_command> del::prepare(request&& req)
{
    if (req._args_count < 1) {
        return unexpected::prepare(std::move(req._command), std::move(bytes {msg_syntax_err}));
    }
    return make_shared<del> (std::move(req._command), std::move(req._args[0]));
}

future<reply> del::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    auto& db = proxy.get_db().local();
    auto schema = db.find_schema(db::system_keyspace::redis::NAME, db::system_keyspace::redis::SIMPLE_OBJECTS);
    // construct the mutation.
    //auto m = mutation_helper::make_mutation(schema, _key);
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(_key)));
    auto m = std::move(mutation(schema, std::move(pkey)));
    m.partition().apply(tombstone { api::new_timestamp(), gc_clock::now() });
    return proxy.mutate_atomically(std::vector<mutation> { m }, cl, timeout, cs.get_trace_state()).then_wrapped([] (future<> f) {
        try {
            f.get();
        } catch (...) {
            // FIXME: what kind of exceptions.
            return reply_builder::build<error_tag>();
        }
        return reply_builder::build<ok_tag>();
    });
}
}
}
