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

shared_ptr<abstract_command> set::prepare(request&& req)
{
    if (req._args_count < 2) {
        return unexpected::prepare(std::move(req._command), std::move(bytes {msg_syntax_err}));
    }
    else if (req._args_count > 2) {
        // FIXME: more other options
    }
    return make_shared<set> (std::move(req._command), std::move(req._args[0]), std::move(req._args[1]));
}

future<reply> set::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    auto& db = proxy.get_db().local();
    auto schema = db.find_schema(db::system_keyspace::redis::NAME, db::system_keyspace::redis::SIMPLE_OBJECTS);
    // construct the mutation.
    //auto m = mutation_helper::make_mutation(schema, _key);
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(_key)));
    auto m = std::move(mutation(schema, std::move(pkey)));
    const column_definition& data_def = *schema->get_column_definition("data");
    // empty clustering key.
    auto data_cell = utf8_type->decompose(make_sstring(_data));
    //m.set_clustered_cell(clustering_key::make_empty(), data_def, atomic_cell::make_live(*utf8_type, 0, std::move(data_cell)));
    m.set_clustered_cell(clustering_key::make_empty(), data_def, atomic_cell::make_live(*utf8_type, api::timestamp_clock::now().time_since_epoch().count(), std::move(data_cell)));
    // call service::storage_proxy::mutate_automicly to apply the mutation.
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
