#include "redis/commands/counter.hh"
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
#include "cql3/query_options.hh"
namespace redis {
namespace commands {
static logging::logger log("command_counter");
shared_ptr<abstract_command> counter_by_prepare_impl(service::storage_proxy& proxy, request&& req, bool incr)
{
    if (req._args_count < 2) {
        return unexpected::prepare(std::move(req._command), std::move(bytes { msg_syntax_err }) );
    }
    return make_shared<counter>(std::move(req._command), simple_objects_schema(proxy), std::move(req._args[0]), std::move(req._args[1]), incr);
}
shared_ptr<abstract_command> counter::prepare(service::storage_proxy& proxy, incrby_tag, request&& req)
{
    return counter_by_prepare_impl(proxy, std::move(req), true);
}
shared_ptr<abstract_command> counter::prepare(service::storage_proxy& proxy, decrby_tag, request&& req)
{
    return counter_by_prepare_impl(proxy, std::move(req), false);
}
shared_ptr<abstract_command> counter::prepare(service::storage_proxy& proxy, incr_tag, request&& req)
{
    req._args_count++;
    req._args.emplace_back(std::move(to_bytes("1")));
    return counter_by_prepare_impl(proxy, std::move(req), true);
}
shared_ptr<abstract_command> counter::prepare(service::storage_proxy& proxy, decr_tag, request&& req)
{
    req._args_count++;
    req._args.emplace_back(std::move(to_bytes("1")));
    return counter_by_prepare_impl(proxy, std::move(req), false);
}

future<reply> counter::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    auto fetched = prefetch_partition_helper::prefetch_simple(proxy, _schema, _key, cl, timeout, cs);
    return fetched.then([this, &proxy, cl, timeout, &cs] (auto pd) {
        long result = 0;
        if (pd && pd->fetched()) {
            if (is_number(pd->_data) == false) {
                return reply_builder::build<error_message_tag>("-ERR value is not an integer or out of range\r\n");
            }
            result = bytes2long(pd->_data);
        }
        if (_incr) result += bytes2long(_data);
        else result -= bytes2long(_data);
        bytes new_data = long2bytes(result);
        log.debug("[2]partition key = {} {}, result = {} {}, data = {} {}", make_sstring(_key), _key.size(), make_sstring(new_data), new_data.size(), make_sstring(_data), _data.size());
        return write_mutation(proxy, _schema, _key, std::move(new_data), cl, timeout, cs).then_wrapped([this, result] (auto f) {
            try {
                f.get();
            } catch(...) {
                return reply_builder::build<error_tag>();
            }
            return reply_builder::build<counter_tag>(result);
        });
    });
}
}
}
