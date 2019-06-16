#include "redis/commands/lrange.hh"
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

shared_ptr<abstract_command> lrange::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 3) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 3, req._args_count);
    }
    return make_shared<lrange>(std::move(req._command), lists_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), bytes2long(req._args[1]), bytes2long(req._args[2]));
}

future<redis_message> lrange::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_list(proxy, _schema, _key, fetch_options::values, false, cl, timeout, cs).then([this] (auto pd) {
        if (_begin < 0) _begin = 0;
        if (pd && pd->has_data()) {
            while (_end < 0 && pd->data().size() > 0) _end += static_cast<long>(pd->data().size());
            if (static_cast<size_t>(_end) >= pd->data().size()) _end = static_cast<long>(pd->data().size()) - 1;
            if (_begin <= _end) {
                return redis_message::make_list_bytes(pd, static_cast<size_t> (_begin), static_cast<size_t> (_end));
            }
        }
        return redis_message::make_empty_list_bytes();
    });
}
}
}
