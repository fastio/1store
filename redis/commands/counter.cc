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
shared_ptr<abstract_command> counter_by_prepare_impl(request&& req, bool incr)
{
    if (req._args_count < 2) {
        return unexpected::prepare(std::move(req._command), std::move(bytes { msg_syntax_err }) );
    }
    return make_shared<counter>(std::move(req._command), std::move(req._args[0]), std::move(req._args[1]), incr);
}
shared_ptr<abstract_command> counter::prepare(incrby_tag, request&& req)
{
    return counter_by_prepare_impl(std::move(req), true);
}
shared_ptr<abstract_command> counter::prepare(decrby_tag, request&& req)
{
    return counter_by_prepare_impl(std::move(req), false);
}
shared_ptr<abstract_command> counter::prepare(incr_tag, request&& req)
{
    req._args_count++;
    req._args.emplace_back(std::move(to_bytes("1")));
    return counter_by_prepare_impl(std::move(req), true);
}
shared_ptr<abstract_command> counter::prepare(decr_tag, request&& req)
{
    req._args_count++;
    req._args.emplace_back(std::move(to_bytes("1")));
    return counter_by_prepare_impl(std::move(req), false);
}

future<reply> counter::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto& db = proxy.get_db().local();
    auto schema = db.find_schema(db::system_keyspace::redis::NAME, db::system_keyspace::redis::SIMPLE_OBJECTS);
    auto timeout = now + tc.read_timeout;
    auto fetched = prefetch_partition_helper::prefetch_simple(proxy, schema, _key, cl, timeout, cs);
    return fetched.then([this, &proxy, cl, timeout, &cs, schema] (auto pd) {
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
        return mutation_helper::write_mutation(proxy, schema, _key, std::move(new_data), cl, timeout, cs).then_wrapped([this, result] (auto f) {
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
