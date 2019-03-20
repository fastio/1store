#include "redis/commands/zcard.hh"
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
#include "log.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/indirected.hpp>
namespace redis {
namespace commands {

shared_ptr<abstract_command> zcard::prepare(service::storage_proxy& proxy, request&& req)
{
    if (req._args_count < 1) {
        return unexpected::prepare(std::move(req._command), std::move(bytes { msg_syntax_err }) );
    }
    return seastar::make_shared<zcard>(std::move(req._command), zsets_schema(proxy), std::move(req._args[0]));
}

future<redis_message> zcard::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_map(proxy, _schema, _key, fetch_options::keys, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        if (pd && pd->has_data()) {
            return redis_message::make(pd->data().size());
        }
        return redis_message::null();
    });
}

}
}
