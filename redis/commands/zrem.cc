#include "redis/commands/zrem.hh"
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

shared_ptr<abstract_command> zrem::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req) {
    if (req._args_count < 2) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 2, req._args_count);
    }
    std::vector<bytes> members;
    members.reserve(req._args_count - 1);
    for (size_t i = 1; i < req._args_count; i++) {
        members.emplace_back(std::move(req._args[i]));
    }
    return seastar::make_shared<zrem>(std::move(req._command), zsets_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), std::move(members));
}

shared_ptr<abstract_command> zremrangebyrank::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req) {
    if (req._args_count < 3) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 3, req._args_count);
    }
    auto begin = bytes2long(req._args[1]);
    auto end = bytes2long(req._args[2]);
    return seastar::make_shared<zremrangebyrank>(std::move(req._command), zsets_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), begin, end);
}

shared_ptr<abstract_command> zremrangebyscore::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req) {
    if (req._args_count < 3) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 3, req._args_count);
    }
    auto min = bytes2double(req._args[1]);
    auto max = bytes2double(req._args[2]);
    return seastar::make_shared<zremrangebyscore>(std::move(req._command), zsets_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), min, max);
}

future<redis_message> zrem::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_map(proxy, _schema, _key, _members, fetch_options::keys, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        size_t total_removed = 0; 
        if (pd && pd->has_data()) {
            // FIXME: We should delete the empty zsets.
            auto&& removed_keys = boost::copy_range<std::vector<bytes>> (pd->data() | boost::adaptors::transformed([] (auto& e) {
                return std::move(*(e.first));
            }));
            total_removed = removed_keys.size();
            if (total_removed == 0) {
                return redis_message::make_long(static_cast<long>(total_removed));
            }
            return redis::write_mutation(proxy, redis::make_zset_dead_cells(_schema, _key, std::move(removed_keys)), cl, timeout, cs).then_wrapped([this, total_removed] (auto f) {
                try {
                    f.get();
                } catch (std::exception& e) {
                    return redis_message::err();
                }
                return redis_message::make_long(static_cast<long>(total_removed));
            });
        }
        return redis_message::make_long(static_cast<long>(total_removed));
    });
}

future<redis_message> zremrangebyrank::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_map(proxy, _schema, _key, fetch_options::all, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        size_t total_removed = 0; 
        if (_begin < 0) _begin = 0;
        while (_end < 0 && pd->data().size() > 0) _end += static_cast<long>(pd->data().size());
        if (_end > 0 && pd->data().size()) _end = _end % static_cast<long>(pd->data().size());
        if (pd && pd->has_data() && _begin <= _end) {
            auto&& result_scores = boost::copy_range<std::vector<std::pair<std::optional<bytes>, double>>> (pd->data() | boost::adaptors::transformed([] (auto& e) {
                return std::move(std::pair<std::optional<bytes>, double>(std::move(e.first), bytes2double(*(e.second))));
            }));
            std::sort(result_scores.begin(), result_scores.end(), [] (auto& e1, auto& e2) { return e1.second < e2.second; });
            if (static_cast<size_t>(_end) < result_scores.size()) {
                result_scores.erase(result_scores.begin() + static_cast<size_t>(_end), result_scores.end());
            }
            if (_begin > 0) {
                result_scores.erase(result_scores.begin(), result_scores.begin() + static_cast<size_t>(_begin));
            }
            auto&& removed_keys = boost::copy_range<std::vector<bytes>> (std::move(result_scores) | boost::adaptors::transformed([] (auto& e) {
                return std::move(*(e.first)); 
            }));
            total_removed = removed_keys.size();
            if (total_removed == 0) {
                return redis_message::make_long(static_cast<long>(total_removed));
            }
            return redis::write_mutation(proxy, redis::make_zset_dead_cells(_schema, _key, std::move(removed_keys)), cl, timeout, cs).then_wrapped([this, total_removed] (auto f) {
                try {
                    f.get();
                } catch (std::exception& e) {
                    return redis_message::err();
                }
                return redis_message::make_long(static_cast<long>(total_removed));
            });
        }
        return redis_message::make_long(static_cast<long>(total_removed));
    });
}

future<redis_message> zremrangebyscore::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_map(proxy, _schema, _key, fetch_options::all, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        size_t total_removed = 0; 
        if (pd && pd->has_data()) {
            auto&& removed_keys = boost::copy_range<std::vector<bytes>> (pd->data() | boost::adaptors::filtered([min = _min, max = _max] (auto& e) {
                auto v = bytes2double(*(e.second));
                return min <= v && v <= max;
            }) | boost::adaptors::transformed([] (auto& e) {
                return std::move(*(e.first));
            }));
            total_removed = removed_keys.size();
            if (total_removed == 0) {
                return redis_message::make_long(static_cast<long>(total_removed));
            }
            return redis::write_mutation(proxy, redis::make_zset_dead_cells(_schema, _key, std::move(removed_keys)), cl, timeout, cs).then_wrapped([this, total_removed] (auto f) {
                try {
                    f.get();
                } catch (std::exception& e) {
                    return redis_message::err();
                }
                return redis_message::make_long(static_cast<long>(total_removed));
            });
        }
        return redis_message::make_long(static_cast<long>(total_removed));
    });
}

}
}
