#include "redis/commands/lpush.hh"
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
#include "cql3/query_options.hh"
namespace redis {
namespace commands {
constexpr const db_clock::time_point lpush::precision_time::REFERENCE_TIME;
thread_local lpush::precision_time lpush::precision_time::_last = {db_clock::time_point::max(), 0};

lpush::precision_time lpush::precision_time::get_next(db_clock::time_point millis) {
    auto next =  millis < _last._millis
        ? precision_time{millis, 9999}
    : precision_time{millis,
        std::max(0, _last._nanos - 1)};
    _last = next;
    return next;
}

shared_ptr<abstract_command> lpush::prepare(service::storage_proxy& proxy, request&& req)
{
    if (req._args_count < 2) {
        return unexpected::prepare(std::move(req._command), std::move(bytes { msg_syntax_err }) );
    }
    std::vector<bytes> values;
    values.reserve(req._args.size() - 1);
    values.insert(values.end(), std::make_move_iterator(++(req._args.begin())), std::make_move_iterator(req._args.end()));
    return seastar::make_shared<lpush>(std::move(req._command), lists_schema(proxy), std::move(req._args[0]), std::move(values));
}

future<reply> lpush::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    /*
    auto& db = proxy.get_db().local();
    auto schema = db.find_schema(db::system_keyspace::redis::NAME, db::system_keyspace::redis::LISTS);
    //auto schema = db.find_schema("test", "user");
    auto m = mutation_helper::make_mutation(schema, _key);
    const column_definition& data_def = *schema->get_column_definition("data");
    auto timeout = now + tc.read_timeout;
    auto&& ltype = static_cast<const list_type_impl*>(data_def.type.get());

    list_type_impl::mutation lm;
    lm.cells.reserve(_datas.size());
    lm.tomb.timestamp = api::timestamp_clock::now().time_since_epoch().count(); 
    lm.tomb.deletion_time = gc_clock::now();
    auto time = precision_time::REFERENCE_TIME - (db_clock::now() - precision_time::REFERENCE_TIME);
    for (auto&& value : _datas) {
        auto&& pt = precision_time::get_next(time);
        auto uuid = utils::UUID_gen::get_time_UUID_bytes(pt._millis.time_since_epoch().count(), pt._nanos);
        lm.cells.emplace_back(bytes(uuid.data(), uuid.size()), params.make_cell(*ltype->value_comparator(), std::move(value), atomic_cell::collection_member::yes));
    }    

    auto list_mut = ltype->serialize_mutation_form(std::move(lm));
    auto c = atomic_cell_or_collection::from_collection_mutation(std::move(list_mut));
    m.set_clustered_cell(clustering_key::make_empty(), data_def, std::move(c));;
    // call service::storage_proxy::mutate_automicly to apply the mutation.
    return proxy.mutate_atomically(std::vector<mutation> { std::move(m) }, cl, timeout, cs.get_trace_state()).then_wrapped([] (future<> f) {
        try {
            f.get();
        } catch (...) {
            // FIXME: what kind of exceptions.
            return reply_builder::build<error_tag>();
        }
        return reply_builder::build<ok_tag>();
    });
    */
    return reply_builder::build<ok_tag>();
}
}
}
