#include "redis/commands/strlen.hh"
#include "redis/commands/unexpected.hh"
#include "redis/reply_builder.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "timeout_config.hh"
#include "service/client_state.hh"
#include "service/storage_proxy.hh"
#include "db/system_keyspace.hh"
#include "partition_slice_builder.hh"
#include "gc_clock.hh"
#include "dht/i_partitioner.hh"
#include "log.hh"
namespace redis {
namespace commands {
shared_ptr<abstract_command> strlen::prepare(request&& req)
{
    if (req._args_count < 1) {
        return unexpected::prepare(std::move(req._command), std::move(bytes { msg_syntax_err }) );
    }
    return make_shared<strlen>(std::move(req._command), std::move(req._args[0]));
}

future<reply> strlen::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto& db = proxy.get_db().local();
    auto schema = db.find_schema(db::system_keyspace::redis::NAME, db::system_keyspace::redis::SIMPLE_OBJECTS);
    auto timeout = now + tc.read_timeout;
    auto fetched = prefetch_partition_helper::prefetch_simple(proxy, schema, _key, cl, timeout, cs);
    return fetched.then([this, &proxy, cl, timeout, &cs, schema] (auto pd) {
        if (pd && pd->fetched()) {
            return reply_builder::build<number_tag>(pd->_data.size());
        }
        return reply_builder::build<number_tag>(size_t { 0 } );
    });
}
}
}
