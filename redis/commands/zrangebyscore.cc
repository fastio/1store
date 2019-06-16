#include "redis/commands/zrangebyscore.hh"
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
template<typename CommandType>
shared_ptr<abstract_command> prepare_impl(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 3) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 3, req._args_count);
    }
    auto min = bytes2double(req._args[1]);
    auto max = bytes2double(req._args[2]);
    long offset = 0, count = -1;
    bool with_scores = false;
    for (size_t i = 3; i < req._args_count;) {
        if (req._args[i] == bytes("withscores")) {
            with_scores = true;
            i++;
        }
        else if (req._args[i] == bytes("limit")) {
            if (req._args_count < i + 3) {
                return unexpected::make_wrong_arguments_exception(std::move(req._command), i + 3, req._args_count);
            }
            offset = bytes2long(req._args[i + 1]);
            count = bytes2long(req._args[i + 2]);
            i += 2;
        } else {
            i++;
        }
    }
    return seastar::make_shared<CommandType>(std::move(req._command), zsets_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), min, max, with_scores, offset, count);
}

shared_ptr<abstract_command> zrangebyscore::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req) {
    return prepare_impl<zrangebyscore>(proxy, cs, std::move(req));
}

shared_ptr<abstract_command> zrevrangebyscore::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req) {
    return prepare_impl<zrevrangebyscore>(proxy, cs, std::move(req));
}

future<redis_message> zrangebyscore::execute_impl(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs, bool reversed)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_map(proxy, _schema, _key, fetch_options::all, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs, reversed] (auto pd) {
        auto results = make_lw_shared<std::vector<std::optional<bytes>>> ();
        if (pd && pd->has_data()) {
            using result_type = std::vector<std::pair<std::optional<bytes>, double>>;
            auto result_scores = boost::copy_range<result_type> (pd->data() | boost::adaptors::filtered([min = _min, max = _max] (auto& e) {
                auto v = bytes2double(*(e.second));
                return min <= v && v <= max;
            }) | boost::adaptors::transformed([] (auto& e) {
                return std::move(std::pair<std::optional<bytes>, double>(std::move(e.first), bytes2double(*(e.second))));
            }));
            if (reversed) {
                std::sort(result_scores.begin(), result_scores.end(), [] (auto& e1, auto& e2) { return e1.second > e2.second; });
            } else {
                std::sort(result_scores.begin(), result_scores.end(), [] (auto& e1, auto& e2) { return e1.second < e2.second; });
            }
            if (_offset > 0) {
                result_scores.erase(result_scores.begin(), result_scores.begin() + static_cast<size_t>(_offset));
            }
            if (result_scores.size() > static_cast<size_t>(_count)) {
                result_scores.erase(result_scores.begin() + static_cast<size_t>(_count), result_scores.end());
            }
            for (auto&& e : result_scores) {
                results->emplace_back(std::move(e.first));
                if (_with_scores) {
                    results->emplace_back(std::move(double2bytes(e.second)));
                }
            }
        }
        return redis_message::make_zset_bytes(results);
    });
}

}
}
