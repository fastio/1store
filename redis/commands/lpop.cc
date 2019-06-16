#include "redis/commands/lpop.hh"
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
namespace redis {
namespace commands {

template<typename PopType>
shared_ptr<abstract_command> prepare_impl(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 1) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 1, req._args_count);
    }
    return make_shared<PopType>(std::move(req._command), lists_schema(proxy, cs.get_keyspace()), std::move(req._args[0]));
}

shared_ptr<abstract_command> lpop::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    return prepare_impl<lpop>(proxy, cs, std::move(req));
}
shared_ptr<abstract_command> rpop::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    return prepare_impl<rpop>(proxy, cs, std::move(req));
}
future<redis_message> lpop::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    return do_execute(proxy, cl, now, tc, cs, true);
}
future<redis_message> rpop::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    return do_execute(proxy, cl, now, tc, cs, false);
}

future<redis_message> pop::do_execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs, bool left)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_list(proxy, _schema, _key, fetch_options::all, !left, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs, left] (auto pd) {
        if (pd && pd->has_data()) {
            auto removed = pd->data().front();
            return [this, removed_cell_key = removed.first, &proxy, &cs, timeout, cl, pd] () {
                // The last cell, delete this partition.
                if (pd->data_size() == 1) {
                    return redis::write_mutation(proxy, redis::make_dead(_schema, _key), cl, timeout, cs);
                }
                std::vector<std::optional<bytes>> removed_cell_keys { std::move(removed_cell_key) };
                return redis::write_mutation(proxy, redis::make_list_dead_cells(_schema, _key, std::move(removed_cell_keys)), cl, timeout, cs);
            } ().then_wrapped([this, pd] (auto f) {
                try {
                    f.get();
                } catch(...) {
                    return redis_message::err();
                }
                return redis_message::make_list_bytes(pd, size_t { 0 } );
            });
        }
        return redis_message::null();
    });
}
}
}
