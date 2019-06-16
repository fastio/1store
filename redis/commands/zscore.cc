#include "redis/commands/zscore.hh"
#include "redis/commands/unexpected.hh"
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
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/indirected.hpp>
namespace redis {
namespace commands {

shared_ptr<abstract_command> zscore::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 2 || req._args_count % 2 != 0) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 2, req._args_count);
    }
    std::vector<bytes> map_keys;
    for (size_t i = 1; i < req._args_count; i++) {
        map_keys.emplace_back(req._args[i]);
    }
    return seastar::make_shared<zscore>(std::move(req._command), zsets_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), std::move(map_keys));
}

future<redis_message> zscore::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_map(proxy, _schema, _key, _map_keys, fetch_options::values, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        if (pd && pd->has_data()) {
            return redis_message::make_map_key_bytes(pd);
        }
        return redis_message::null();
    });
}

}
}
