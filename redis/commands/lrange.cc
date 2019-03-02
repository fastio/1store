#include "redis/commands/lrange.hh"
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

shared_ptr<abstract_command> lrange::prepare(service::storage_proxy& proxy, request&& req)
{
    if (req._args_count < 3) {
        return unexpected::prepare(std::move(req._command), std::move(bytes { msg_syntax_err }) );
    }
    return make_shared<lrange>(std::move(req._command), lists_schema(proxy), std::move(req._args[0]), bytes2long(req._args[1]), bytes2long(req._args[2]));
}

future<reply> lrange::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_list(proxy, _schema, _key, cl, timeout, cs, _start, _end).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        if (pd && pd->has_data()) {
            std::vector<bytes> values;
            values.reserve(pd->data_size());
            for (auto& c : pd->data()) {
                values.emplace_back(std::move(c.second));
            }
            return reply_builder::build(std::move(values));
        }
        return reply_builder::build<null_message_tag>();
    });
}
}
}
