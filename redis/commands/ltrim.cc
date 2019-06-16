#include "redis/commands/ltrim.hh"
#include "redis/commands/unexpected.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "timeout_config.hh"
#include "service/client_state.hh"
#include "service/storage_proxy.hh"
#include "db/system_keyspace.hh"
#include "partition_slice_builder.hh"
#include "gc_clock.hh"
#include "dht/i_partitioner.hh"
#include "redis/prefetcher.hh"
#include "redis/redis_mutation.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/indirected.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/range/adaptor/reversed.hpp>
namespace redis {
namespace commands {

shared_ptr<abstract_command> ltrim::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 3) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 3, req._args_count);
    }
    auto begin = bytes2long(req._args[1]);
    auto end = bytes2long(req._args[2]);
    if (begin < 0 || end < 0) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 3, req._args_count);
    }
    return make_shared<ltrim>(std::move(req._command), lists_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), begin, end);
}

future<redis_message> ltrim::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_list(proxy, _schema, _key, fetch_options::all, false, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        if (pd && pd->has_data()) {
            std::vector<std::optional<bytes>> removed;
            auto& data = pd->data();
            auto size = data.size();
            auto begin = static_cast<size_t>(_begin);
            auto end = static_cast<size_t>(_end);
            if (end >= size) {
                end = size - 1;
            }
            return [this, size, begin, end, pd, &proxy, cl, timeout, &cs]  {
                // Delete the parition.
                if (begin >= end) {
                    return redis::write_mutation(proxy, redis::make_dead(_schema, _key), cl, timeout, cs);
                }
                else {
                    auto removed_keys = boost::copy_range<std::vector<std::optional<bytes>>> (pd->data() | boost::adaptors::filtered([this, begin, end] (auto&) {
                        auto r = _index >= begin && _index <= end;
                        _index++;
                        return !r;
                    }) | boost::adaptors::transformed([this] (auto& e) {
                        return std::move(e.first);
                    }));
                    return redis::write_mutation(proxy, redis::make_list_dead_cells(_schema, _key, std::move(removed_keys)), cl, timeout, cs);
                }
            } ().then_wrapped([] (auto f) {
                try {
                    f.get();
                } catch(...) {
                    return redis_message::err();
                }
                return redis_message::ok();
            });
        }
        return redis_message::ok();
    });
}

} // end of commands
} // end of redis
