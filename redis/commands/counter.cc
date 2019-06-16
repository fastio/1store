#include "redis/commands/counter.hh"
#include "redis/commands/unexpected.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "redis/redis_mutation.hh"
#include "timeout_config.hh"
#include "service/client_state.hh"
#include "service/storage_proxy.hh"
#include "db/system_keyspace.hh"
#include "partition_slice_builder.hh"
#include "gc_clock.hh"
#include "dht/i_partitioner.hh"
#include "redis/prefetcher.hh"
#include "cql3/query_options.hh"
namespace redis {
namespace commands {
shared_ptr<abstract_command> counter_by_prepare_impl(service::storage_proxy& proxy, const service::client_state& cs, request&& req, bool incr)
{
    if (req._args_count < 2) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 2, req._args_count);
    }
    return make_shared<counter>(std::move(req._command), simple_objects_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), std::move(req._args[1]), incr);
}
shared_ptr<abstract_command> counter::prepare(service::storage_proxy& proxy, const service::client_state& cs, incrby_tag, request&& req)
{
    return counter_by_prepare_impl(proxy, cs, std::move(req), true);
}
shared_ptr<abstract_command> counter::prepare(service::storage_proxy& proxy, const service::client_state& cs, decrby_tag, request&& req)
{
    return counter_by_prepare_impl(proxy, cs, std::move(req), false);
}
shared_ptr<abstract_command> counter::prepare(service::storage_proxy& proxy, const service::client_state& cs, incr_tag, request&& req)
{
    req._args_count++;
    req._args.emplace_back(std::move(to_bytes("1")));
    return counter_by_prepare_impl(proxy, cs, std::move(req), true);
}
shared_ptr<abstract_command> counter::prepare(service::storage_proxy& proxy, const service::client_state& cs, decr_tag, request&& req)
{
    req._args_count++;
    req._args.emplace_back(std::move(to_bytes("1")));
    return counter_by_prepare_impl(proxy, cs, std::move(req), false);
}

future<redis_message> counter::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_simple(proxy, _schema, _key, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        long result = 0;
        if (pd && pd->has_data()) {
            if (is_number(pd->_data) == false) {
                return redis_message::make_exception(sstring("-ERR value is not an integer or out of range\r\n"));
            }
            result = bytes2long(pd->_data);
        }
        if (_incr) result += bytes2long(_data);
        else result -= bytes2long(_data);
        bytes new_data = long2bytes(result);
        return redis::write_mutation(proxy, redis::make_simple(_schema, _key, std::move(new_data)), cl, timeout, cs).then_wrapped([this, result] (auto f) {
            try {
                f.get();
            } catch(...) {
                return redis_message::err(); 
            }
            return redis_message::make_long(result);
        });
    });
}
}
}
