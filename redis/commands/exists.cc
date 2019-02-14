#include "redis/commands/exists.hh"
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
shared_ptr<abstract_command> exists::prepare(request&& req)
{
    if (req._args_count < 1) {
        return unexpected::prepare(std::move(req._command), std::move(bytes { msg_syntax_err }) );
    }
    return make_shared<exists>(std::move(req._command), std::move(req._args[0]));
}

future<reply> exists::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    return do_execute_with_action(proxy, cl, now, tc, cs, [this] (schema_ptr schema, query_result_type&& q_result) {
        if (!q_result) {
            return reply_builder::build<error_tag>();
        }    
        auto& partition_count = q_result->partition_count();
        if (partition_count && *partition_count > 0) {
            auto full_slice = partition_slice_builder(*schema).build();
            auto result_s = query::result_set::from_raw_result(schema, full_slice, *(q_result));
            const auto& row = result_s.row(0);
            if (row.has(sstring("data"))) {
                return reply_builder::build<ok_tag>();
            }
        }
        return reply_builder::build<error_tag>();
    });
}
}
}
