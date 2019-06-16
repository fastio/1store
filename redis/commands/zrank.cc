#include "redis/commands/zrank.hh"
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

template<typename RankType>
shared_ptr<abstract_command> prepare_impl(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 2) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 2, req._args_count);
    }
    return seastar::make_shared<RankType>(std::move(req._command), zsets_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), std::move(req._args[1]));
}
shared_ptr<abstract_command> zrank::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req) {
    return prepare_impl<zrank>(proxy, cs, std::move(req));
}
shared_ptr<abstract_command> zrevrank::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req) {
    return prepare_impl<zrevrank>(proxy, cs, std::move(req));
}

future<redis_message> zrank::execute_impl(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs, bool reversed)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_map(proxy, _schema, _key, fetch_options::all, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs, reversed] (auto pd) {
        if (pd && pd->has_data()) {
            auto target = std::find_if(pd->data().begin(), pd->data().end(), [this] (auto& e) { return *(e.first) == _member; });
            if (target != pd->data().end()) {
                auto v = bytes2double(*(target->second));
                size_t rank = 0;
                for_each(pd->data().begin(), pd->data().end(), [v, &rank, reversed] (auto& e) {
                    auto ev = bytes2double(*(e.second));
                    // FIXME: if ev equal to v, ordered by ckey.
                    if (!reversed) {
                        if (ev <= v) ++rank;
                    } else {
                        if (ev >= v) ++rank;
                    }
                });
                --rank;
                return redis_message::make_long(static_cast<long>(rank));
            }
        }
        return redis_message::null();
    });
}

}
}
