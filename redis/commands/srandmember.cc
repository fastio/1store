#include "redis/commands/srandmember.hh"
#include "redis/commands/unexpected.hh"
#include "seastar/core/shared_ptr.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "db/system_keyspace.hh"
#include "types.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "mutation.hh"
#include "timeout_config.hh"
#include "redis/redis_mutation.hh"
#include "redis/prefetcher.hh"
#include <random>
namespace redis {

namespace commands {

shared_ptr<abstract_command> srandmember::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 1) {
        return unexpected::make_exception(std::move(req._command), sprint("-wrong number of arguments (given %ld, expected 1)\r\n", req._args_count));
    }
    long count = 0;
    if (req._args_count > 1) {
        count = bytes2long(req._args[1]);
    }
    return seastar::make_shared<srandmember> (std::move(req._command), sets_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), count);
}

future<redis_message> srandmember::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_set(proxy, _schema, _key, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        if (pd && pd->has_data()) {
            if (_count > 0 && pd->data().size() <= static_cast<size_t>(_count)) {
                return redis_message::make_map_key_bytes(pd);
            }
            auto make_random_indexes = [] (int max, long count) {
                std::vector<size_t> indexes;
                auto gen = std::default_random_engine(std::random_device()());
                auto dist = std::uniform_int_distribution(0, max);
                if (count == 0) {
                    indexes.emplace_back(static_cast<size_t>(dist(gen) % max));
                } else if (count > 0) {
                    indexes.reserve(count);
                    // generate the sequence of unique index.
                    std::unordered_set<int> temp;
                    while (temp.size() < static_cast<size_t>(count)) {
                        temp.emplace(static_cast<size_t>(dist(gen) % max)); 
                    }
                    for (auto e : temp) {
                        indexes.emplace_back(e);
                    }
                } else {
                    auto abs_count = std::abs(count);
                    indexes.reserve(abs_count);
                    while (indexes.size() < static_cast<size_t>(abs_count)) {
                        indexes.emplace_back(static_cast<size_t>(dist(gen) % max)); 
                    }
                }
                return std::move(indexes);
            };
            auto indexes = make_random_indexes(pd->data().size(), _count);
            return redis_message::make_set_bytes(pd, std::move(indexes)); 
        }
        return redis_message::null(); 
    });
}

}
}
