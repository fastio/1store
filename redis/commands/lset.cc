#include "redis/commands/lset.hh"
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

shared_ptr<abstract_command> lset::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 3) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 3, req._args_count);
    }
    return make_shared<lset>(std::move(req._command), lists_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), bytes2long(req._args[1]), std::move(req._args[2]));
}

future<redis_message> lset::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    return prefetch_list(proxy, _schema, _key, fetch_options::keys, false, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto pd) {
        if (pd && pd->has_data()) {
            auto& e = pd->data().front();
            std::vector<std::pair<std::optional<bytes>, std::optional<bytes>>> new_cells { { std::move(e.first), std::move(e.second) } };
            return redis::write_mutation(proxy, redis::make_list_indexed_cells(_schema, _key, std::move(new_cells)), cl, timeout, cs).then_wrapped([this] (auto f) {
                try {
                    f.get();
                } catch(std::exception& e) {
                    return redis_message::null();
                }
                return redis_message::ok();
            });
        }
        std::vector<bytes> data { std::move(_value) };
        return redis::write_mutation(proxy, redis::make_list_cells(_schema, _key, std::move(data), true), cl, timeout, cs).then_wrapped([this] (auto f) {
            try {
                f.get();
            } catch(...) {
                return redis_message::null();
            }
            return redis_message::ok();
        });
    });
}
}
}
