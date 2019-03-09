#include "redis/commands/llen.hh"
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
#include "redis/prefetcher.hh"
namespace redis {
namespace commands {

shared_ptr<abstract_command> llen::prepare(service::storage_proxy& proxy, request&& req)
{
    if (req._args_count < 1) {
        return unexpected::prepare(std::move(req._command), std::move(bytes { msg_syntax_err }) );
    }
    return make_shared<llen>(std::move(req._command), lists_schema(proxy), std::move(req._args[0]));
}

future<reply> llen::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_list(proxy, _schema, _key, fetch_options::values, cl, timeout, cs).then([] (auto pd) {
        if (pd && pd->has_data()) {
            return reply_builder::build<number_tag>(static_cast<size_t>(pd->data().size()));
        }
        return reply_builder::build<null_message_tag>();
    });
}
}
}
