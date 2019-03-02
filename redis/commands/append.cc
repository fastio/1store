#include "redis/commands/append.hh"
#include "redis/commands/unexpected.hh"
#include "redis/reply_builder.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "redis/redis_mutation.hh"
#include "redis/prefetcher.hh"
#include "timeout_config.hh"
#include "service/client_state.hh"
#include "service/storage_proxy.hh"
#include "db/system_keyspace.hh"
#include "partition_slice_builder.hh"
#include "gc_clock.hh"
#include "dht/i_partitioner.hh"
#include "cql3/query_options.hh"
namespace redis {
namespace commands {
shared_ptr<abstract_command> append::prepare(service::storage_proxy& proxy, request&& req)
{
    if (req._args_count < 2) {
        return unexpected::prepare(std::move(req._command), std::move(bytes { msg_syntax_err }) );
    }
    return make_shared<append>(std::move(req._command), simple_objects_schema(proxy), std::move(req._args[0]), std::move(req._args[1]));
}

future<reply> append::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_simple(proxy, _schema, _key, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        bytes new_data;
        if (pd && pd->has_data()) {
            new_data = std::move(pd->_data + _data);
        } else {
            new_data = std::move(_data);
        }
        return redis::write_mutation(proxy, redis::make_simple(_schema, _key, std::move(new_data)), cl, timeout, cs).then_wrapped([this] (auto f) {
            try {
                f.get();
            } catch(...) {
                return reply_builder::build<error_tag>();
            }
            return reply_builder::build<ok_tag>();
        });
    });
}
}
}
