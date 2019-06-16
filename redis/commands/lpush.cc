#include "redis/commands/lpush.hh"
#include "redis/commands/unexpected.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "redis/redis_mutation.hh"
#include "timeout_config.hh"
#include "service/client_state.hh"
#include "service/storage_proxy.hh"
#include "db/system_keyspace.hh"
#include "partition_slice_builder.hh"
#include "gc_clock.hh"
#include "dht/i_partitioner.hh"
#include "redis/prefetcher.hh"
#include "cql3/query_options.hh"
namespace redis {
namespace commands {
template<typename PushType>
shared_ptr<abstract_command> prepare_impl(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 2) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 2, req._args_count);
    }
    std::vector<bytes> values;
    values.reserve(req._args.size() - 1);
    values.insert(values.end(), std::make_move_iterator(++(req._args.begin())), std::make_move_iterator(req._args.end()));
    return seastar::make_shared<PushType>(std::move(req._command), lists_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), std::move(values));
}

shared_ptr<abstract_command> lpush::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{   
    return prepare_impl<lpush>(proxy, cs, std::move(req));
}

shared_ptr<abstract_command> lpushx::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    return prepare_impl<lpushx>(proxy, cs, std::move(req));
}

shared_ptr<abstract_command> rpush::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    return prepare_impl<rpush>(proxy, cs, std::move(req));
}

shared_ptr<abstract_command> rpushx::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    return prepare_impl<rpushx>(proxy, cs, std::move(req));
}


future<redis_message> push::do_execute(service::storage_proxy& proxy,
    db::consistency_level cl,
    db::timeout_clock::time_point now,
    const timeout_config& tc,
    service::client_state& cs,
    bool left)
{
    return check_exists(proxy, cl, now, tc, cs).then([this, &proxy, cl, now, tc, &cs, left] (auto write) {
        if (write == false) {
            return redis_message::err();
        }
        auto timeout = now + tc.read_timeout;
        auto total = _data.size();
        return redis::write_mutation(proxy, redis::make_list_cells(_schema, _key, std::move(_data), left), cl, timeout, cs).then_wrapped([this, total] (auto f) {
            try {
                f.get();
            } catch (...) {
                // FIXME: what kind of exceptions.
                return redis_message::err();
            }
            return redis_message::make_long(static_cast<long>(total));
        });
    });
}


future<redis_message> lpush::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    return do_execute(proxy, cl, now, tc, cs, true);
}

future<redis_message> rpush::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    return do_execute(proxy, cl, now, tc, cs, false);
}


future<bool> lpushx::check_exists(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    return exists(proxy, _schema, _key, cl, timeout, cs);
}

future<redis_message> lpushx::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    return do_execute(proxy, cl, now, tc, cs, true);
}

future<redis_message> rpushx::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    return do_execute(proxy, cl, now, tc, cs, false);
}

}
}
