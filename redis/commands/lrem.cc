#include "redis/commands/lrem.hh"
#include "redis/commands/unexpected.hh"
#include "redis/reply_builder.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "timeout_config.hh"
#include "service/client_state.hh"
#include "service/storage_proxy.hh"
#include "db/system_keyspace.hh"
#include "partition_slice_builder.hh"
#include "gc_clock.hh"
#include "dht/i_partitioner.hh"
#include "log.hh"
namespace redis {
namespace commands {

shared_ptr<abstract_command> lrem::prepare(service::storage_proxy& proxy, request&& req)
{
    if (req._args_count < 3) {
        return unexpected::prepare(std::move(req._command), std::move(bytes { msg_syntax_err }) );
    }
    return make_shared<lrem>(std::move(req._command), lists_schema(proxy), std::move(req._args[0]), std::move(req._args[2]), bytes2long(req._args[1]));
}

future<reply> lrem::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.read_timeout;
    auto fetched = prefetch_partition_helper::prefetch_list(proxy, _schema, _key, cl, timeout, cs, std::move(_target), _count);
    return fetched.then([this, &proxy, cl, timeout, &cs] (auto pd) {
        if (pd && pd->fetched()) {
            return [this, &proxy, &cs, timeout, cl, pd] () {
                // The last cell, delete this partition.
                if (!pd->has_more()) {
                    return write_mutation(proxy, _schema, _key, partition_dead_tag {}, cl, timeout, cs);
                }
                auto dead_cell_keys = boost::copy_range<std::vector<bytes>>(pd->cells() | boost::adaptors::transformed([] (auto& c) { return std::move(c._key); }));
                return write_list_dead_cell_mutation(proxy, _schema, _key, std::move(dead_cell_keys), cl, timeout, cs);
            } ().then_wrapped([this, pd] (auto f) {
                try {
                    f.get();
                } catch(...) {
                    return reply_builder::build<error_tag>();
                }
                auto values = boost::copy_range<std::vector<bytes>>(pd->cells() | boost::adaptors::transformed([] (auto& c) { return std::move(c._value); }));
                return reply_builder::build(std::move(values));
            });
        }
        return reply_builder::build<null_message_tag>();
    });
}
}
}
