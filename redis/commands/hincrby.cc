#include "redis/commands/hincrby.hh"
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

shared_ptr<abstract_command> hincrby::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 3) {
        return unexpected::make_exception(std::move(req._command), sprint("-wrong number of arguments (given %ld, expected 1)\r\n", req._args_count));
    }
    return seastar::make_shared<hincrby>(std::move(req._command), maps_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), std::move(req._args[1]), bytes2double(req._args[2]));
}

future<redis_message> hincrby::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_map(proxy, _schema, _key, std::vector<bytes> { _field }, fetch_options::values, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        double result = 0; 
        if (pd && pd->has_data()) {
            auto&& existing = pd->data().front().first;
            result = bytes2double(*existing);
        }
        result += _increment;
        auto new_value = double2bytes(result);
        std::unordered_map<bytes, bytes> data { std::make_pair(std::move(_field), std::move(new_value)) };
        return redis::write_mutation(proxy, redis::make_map_cells(_schema, _key, std::move(data)), cl, timeout, cs).then_wrapped([this, result] (auto f) {
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
