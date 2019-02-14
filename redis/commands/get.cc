#include "redis/commands/get.hh"
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
static logging::logger log("command_get");
shared_ptr<abstract_command> get::prepare(request&& req)
{
    if (req._args_count < 1) {
        return unexpected::prepare(std::move(req._command), std::move(bytes { msg_syntax_err }) );
    }
    return make_shared<get>(std::move(req._command), std::move(req._args[0]));
}

using query_result_type = foreign_ptr<lw_shared_ptr<query::result>>;
future<reply> get::do_execute_with_action(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now,
        const timeout_config& tc, service::client_state& cs, std::function<future<reply>(schema_ptr, query_result_type&&)> action)
{
    auto timeout = now + tc.read_timeout;
    // find the schema.
    auto& db = proxy.get_db().local();
    auto schema = db.find_schema(db::system_keyspace::redis::NAME, db::system_keyspace::redis::SIMPLE_OBJECTS);
    // create the read command.
    auto full_slice = partition_slice_builder(*schema).build();
    auto pkey = partition_key::from_single_value(*schema, utf8_type->decompose(make_sstring(_key)));
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(),
        full_slice, std::numeric_limits<int32_t>::max(), gc_clock::now(), tracing::make_trace_info(cs.get_trace_state()), query::max_partitions, utils::UUID(), cs.get_timestamp());

    // consume the result, and convert it to redis format.
    auto partition_range = dht::partition_range::make_singular(dht::global_partitioner().decorate_key(*schema, std::move(pkey)));
    dht::partition_range_vector partition_ranges;
    partition_ranges.emplace_back(std::move(partition_range));
    return proxy.query(schema, command, std::move(partition_ranges), cl, {timeout, cs.get_trace_state()}).then([full_slice = std::move(full_slice), schema, action = std::move(action)] (auto q_result) {
        return action(schema, std::move(q_result.query_result));
    });
}

future<reply> get::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    return do_execute_with_action(proxy, cl, now, tc, cs, [this] (schema_ptr schema, query_result_type&& q_result) {
        if (!q_result) {
            return reply_builder::build<null_message_tag>();
        }    
        auto& partition_count = q_result->partition_count();
        if (partition_count && *partition_count > 0) {
            auto full_slice = partition_slice_builder(*schema).build();
            auto result_s = query::result_set::from_raw_result(schema, full_slice, *(q_result));
            const auto& row = result_s.row(0);
            if (row.has(sstring("data"))) {
                const auto& data = row.get_data_value("data");
                auto b = utf8_type->decompose(data);
                auto s = make_sstring(b);
                return reply_builder::build<message_tag>(std::move(b));
            }
        }
        return reply_builder::build<null_message_tag>();
    });
}
}
}
