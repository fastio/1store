#include "redis/commands/zcount.hh"
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
#include "log.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/indirected.hpp>
namespace redis {
namespace commands {

shared_ptr<abstract_command> zcount::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 3) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 3, req._args_count);
    }
    auto min = bytes2double(req._args[1]);
    auto max = bytes2double(req._args[2]);
    return seastar::make_shared<zcount>(std::move(req._command), zsets_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), min, max);
}

future<redis_message> zcount::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_map(proxy, _schema, _key, fetch_options::values, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        size_t count = 0;
        if (pd && pd->has_data()) {
            for_each(pd->data().begin(), pd->data().end(), [min = _min, max = _max, &count] (auto& e) {
                auto v = bytes2double(*(e.first));
                if (min <= v && v <= max) count++;
            });
            return redis_message::make_long(static_cast<long>(count));
        }
        return redis_message::null();
    });
}

}
}
