#include "redis/commands/lrem.hh"
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

shared_ptr<abstract_command> lrem::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 3) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 3, req._args_count);
    }
    return make_shared<lrem>(std::move(req._command), lists_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), std::move(req._args[2]), bytes2long(req._args[1]));
}

future<redis_message> lrem::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_list(proxy, _schema, _key, fetch_options::all, false, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        if (pd && pd->has_data()) {
            std::vector<std::optional<bytes>> removed;
            auto& data = pd->data();
            size_t c = 0, i = 0;
            if (_count < 0) {
                c = 0 - static_cast<size_t>(_count);
                for (auto&& el : data | boost::adaptors::reversed) {
                    if (i >= c) {
                        break;
                    }
                    if (*(el.second) == _target && i < c) {
                        ++i;
                        removed.emplace_back(std::move(el.first)); 
                    }
                }
            } else {
                c = static_cast<size_t>(_count) - 1;
                for (auto&& el : data) {
                    if (c >= 0 && i > c) break;
                    if (*(el.second) == _target) {
                        ++i;
                        removed.emplace_back(std::move(el.first)); 
                    }
                }
            }
            auto total = removed.size();
            return [this, removed_cell_keys = std::move(removed), &proxy, &cs, timeout, cl, pd] () {
                // The last cell, delete this partition.
                if (pd->data_size() == removed_cell_keys.size()) {
                    return redis::write_mutation(proxy, redis::make_dead(_schema, _key), cl, timeout, cs);
                }
                std::vector<std::optional<bytes>> removed_keys = removed_cell_keys;
                return redis::write_mutation(proxy, redis::make_list_dead_cells(_schema, _key, std::move(removed_keys)), cl, timeout, cs);
            } ().then_wrapped([this, total, pd] (auto f) {
                try {
                    f.get();
                } catch(...) {
                    return redis_message::err();
                }
                return redis_message::make_long(static_cast<long>(total));
            });
        }
        return redis_message::null(); 
    });
}

} // end of commands
} // end of redis
