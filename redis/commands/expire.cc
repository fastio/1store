#include "redis/commands/expire.hh"
#include "redis/commands/unexpected.hh"
#include "seastar/core/shared_ptr.hh"
#include "redis/request.hh"
#include "redis/redis_mutation.hh"
#include "redis/reply.hh"
#include "db/system_keyspace.hh"
#include "types.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "mutation.hh"
#include "timeout_config.hh"
#include "redis/prefetcher.hh"
namespace service {
class storage_proxy;
}
namespace redis {

namespace commands {

shared_ptr<abstract_command> expire::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count != 2) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 2, req._args_count);
    }
    long ttl = 0;
    try {
        ttl = bytes2long(req._args[1]);
    } catch(std::exception&) {
        return unexpected::make_exception(std::move(req._command), "-ERR value is not an integer or out of range");
    }
    std::vector<schema_ptr> schemas {
        simple_objects_schema(proxy, cs.get_keyspace()),
        lists_schema(proxy, cs.get_keyspace()),
        sets_schema(proxy, cs.get_keyspace()),
        maps_schema(proxy, cs.get_keyspace()),
        zsets_schema(proxy, cs.get_keyspace())
    };
    return seastar::make_shared<expire> (std::move(req._command), std::move(schemas), std::move(req._args[0]), ttl);
}

shared_ptr<abstract_command> persist::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count != 1) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 1, req._args_count);
    }
    std::vector<schema_ptr> schemas {
        simple_objects_schema(proxy, cs.get_keyspace()),
        lists_schema(proxy, cs.get_keyspace()),
        sets_schema(proxy, cs.get_keyspace()),
        maps_schema(proxy, cs.get_keyspace()),
        zsets_schema(proxy, cs.get_keyspace())
    };
    return seastar::make_shared<persist> (std::move(req._command), std::move(schemas), std::move(req._args[0]));
}

future<redis_message> expire::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    std::vector<std::function<future<bool>()>> executors {
        [this, timeout, &proxy, cl, &cs] () {
            return prefetch_simple(proxy, redis::simple_objects_schema(proxy, cs.get_keyspace()), _key, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
                if (pd && pd->has_data()) {
                    return redis::write_mutation(proxy, redis::make_simple(redis::simple_objects_schema(proxy, cs.get_keyspace()), _key, std::move(pd->_data), _ttl), cl, timeout, cs).then([] {
                        return make_ready_future<bool>(true);
                    });
                }
                return make_ready_future<bool>(false);
            });
        },
        [this, timeout, &proxy, cl, &cs] () {
            return prefetch_list(proxy, redis::lists_schema(proxy, cs.get_keyspace()), _key, fetch_options::all, false, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
                if (pd && pd->has_data()) {
                    auto list_cells = redis::make_list_indexed_cells(redis::lists_schema(proxy, cs.get_keyspace()), _key, std::move(pd->_data), _ttl);
                    return redis::write_mutation(proxy, list_cells, cl, timeout, cs).then([] {
                        return make_ready_future<bool>(true);
                    });
                }
                return make_ready_future<bool>(false);
            });
        },
        [this, timeout, &proxy, cl, &cs] () {
            return prefetch_map(proxy, redis::maps_schema(proxy, cs.get_keyspace()), _key, fetch_options::all, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
                if (pd && pd->has_data()) {
                    auto map_cells = redis::make_map_indexed_cells(redis::maps_schema(proxy, cs.get_keyspace()), _key, std::move(pd->_data), _ttl);
                    return redis::write_mutation(proxy, map_cells, cl, timeout, cs).then([] {
                        return make_ready_future<bool>(true);
                    });
                }
                return make_ready_future<bool>(false);
            });
        },
        [this, timeout, &proxy, cl, &tc, &cs] () {
            return prefetch_set(proxy, redis::sets_schema(proxy, cs.get_keyspace()), _key, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
                if (pd && pd->has_data()) {
                    auto set_cells = redis::make_set_indexed_cells(redis::sets_schema(proxy, cs.get_keyspace()), _key, std::move(pd->_data), _ttl);
                    return redis::write_mutation(proxy, set_cells, cl, timeout, cs).then([] {
                        return make_ready_future<bool>(true);
                    });
                }
                return make_ready_future<bool>(false);
            });
        },
        [this, timeout, &proxy, cl, &tc, &cs] () {
            return prefetch_zset(proxy, redis::zsets_schema(proxy, cs.get_keyspace()), _key, fetch_options::all, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
                if (pd && pd->has_data()) {
                    auto zset_cells = redis::make_zset_indexed_cells(redis::zsets_schema(proxy, cs.get_keyspace()), _key, std::move(pd->_data), _ttl);
                    return redis::write_mutation(proxy, zset_cells, cl, timeout, cs).then([] {
                        return make_ready_future<bool>(true);
                    });
                }
                return make_ready_future<bool>(false);
            });
        }
    };
    return do_with(std::move(executors), bool { false }, [] (auto&& executors, auto&& result) {
        return parallel_for_each(executors.begin(), executors.end(), [&result] (auto& executor) {
            return executor().then([&result] (auto r) { result |= r; });
        }).then([&result] {
            if (result) {
                return redis_message::one();
            }
            return redis_message::zero();
        });
    });
}
}
}
