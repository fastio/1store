#include "redis/commands/zincrby.hh"
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

shared_ptr<abstract_command> zincrby::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 3) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 3, req._args_count);
    }
    return seastar::make_shared<zincrby>(std::move(req._command), zsets_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), std::move(req._args[2]), bytes2double(req._args[1]));
}

future<redis_message> zincrby::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_map(proxy, _schema, _key, std::vector<bytes> { _member }, fetch_options::values, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        double result = 0; 
        if (pd && pd->has_data()) {
            auto&& existing = pd->data().front().first;
            result = bytes2double(*existing);
        }
        result += _increment;
        auto new_value = double2bytes(result);
        std::vector<std::pair<bytes, bytes>> data { std::make_pair(std::move(_member), std::move(new_value)) };
        return redis::write_mutation(proxy, redis::make_zset_cells(_schema, _key, std::move(data)), cl, timeout, cs).then_wrapped([this, result] (auto f) {
            try {
                f.get();
            } catch (std::exception& e) {
                return redis_message::err();
            }
            return redis_message::make_bytes(double2bytes(result));
        });
    });
}

}
}
