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
#include "redis/prefetcher.hh"
#include "cql3/query_options.hh"
namespace redis {
namespace commands {
template<typename PushType>
shared_ptr<abstract_command> prepare_impl(service::storage_proxy& proxy, request&& req)
{
    if (req._args_count < 2) {
        return unexpected::prepare(std::move(req._command), std::move(bytes { msg_syntax_err }) );
    }
    std::vector<bytes> values;
    values.reserve(req._args.size() - 1);
    values.insert(values.end(), std::make_move_iterator(++(req._args.begin())), std::make_move_iterator(req._args.end()));
    return seastar::make_shared<PushType>(std::move(req._command), lists_schema(proxy), std::move(req._args[0]), std::move(values));
}

shared_ptr<abstract_command> lpush::prepare(service::storage_proxy& proxy, request&& req)
{   
    return prepare_impl<lpush>(proxy, std::move(req));
}

shared_ptr<abstract_command> lpushx::prepare(service::storage_proxy& proxy, request&& req)
{
    return prepare_impl<lpushx>(proxy, std::move(req));
}

shared_ptr<abstract_command> rpush::prepare(service::storage_proxy& proxy, request&& req)
{
    return prepare_impl<rpush>(proxy, std::move(req));
}

shared_ptr<abstract_command> rpushx::prepare(service::storage_proxy& proxy, request&& req)
{
    return prepare_impl<rpushx>(proxy, std::move(req));
}


future<reply> push::do_execute(service::storage_proxy& proxy,
    db::consistency_level cl,
    db::timeout_clock::time_point now,
    const timeout_config& tc,
    service::client_state& cs,
    bool left)
{
    return check_exists(proxy, cl, now, tc, cs).then([this, &proxy, cl, now, tc, &cs, left] (auto write) {
        if (write == false) {
            return reply_builder::build<error_tag>();
        }
        auto timeout = now + tc.read_timeout;
        return write_list_mutation(proxy, _schema, _key, std::move(_datas), cl, timeout, cs, left).then_wrapped([this, total = _datas.size()] (auto f) {
            try {
                f.get();
            } catch (...) {
                // FIXME: what kind of exceptions.
                return reply_builder::build<error_tag>();
            }
            return reply_builder::build<number_tag>(total);
        });
    });
}


future<reply> lpush::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    return do_execute(proxy, cl, now, tc, cs, true);
}

future<reply> rpush::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    return do_execute(proxy, cl, now, tc, cs, false);
}


future<bool> lpushx::check_exists(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    return exists(proxy, _schema, _key, cl, timeout, cs);
}

future<reply> lpushx::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    return do_execute(proxy, cl, now, tc, cs, true);
}

future<reply> rpushx::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    return do_execute(proxy, cl, now, tc, cs, false);
}

}
}
