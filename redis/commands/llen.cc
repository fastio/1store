#include "redis/commands/llen.hh"
#include "redis/commands/unexpected.hh"
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

shared_ptr<abstract_command> llen::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 1) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 1, req._args_count);
    }
    return make_shared<llen>(std::move(req._command), lists_schema(proxy, cs.get_keyspace()), std::move(req._args[0]));
}

future<redis_message> llen::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_list(proxy, _schema, _key, fetch_options::values, false, cl, timeout, cs).then([] (auto pd) {
        if (pd && pd->has_data()) {
            return redis_message::make_long(static_cast<long>(pd->data().size()));
        }
        return redis_message::null();
    });
}
}
}
