#include "redis/commands/set.hh"
#include "redis/commands/unexpected.hh"
#include "seastar/core/shared_ptr.hh"
#include "redis/reply_builder.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "db/system_keyspace.hh"
#include "types.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "mutation.hh"
#include "timeout_config.hh"
#include "redis/redis_mutation.hh"
namespace redis {

namespace commands {

shared_ptr<abstract_command> set::prepare(service::storage_proxy& proxy, request&& req)
{
    if (req._args_count < 2) {
        return unexpected::prepare(std::move(req._command), std::move(bytes {msg_syntax_err}));
    }
    else if (req._args_count > 2) {
        // FIXME: more other options
    }
    return seastar::make_shared<set> (std::move(req._command), simple_objects_schema(proxy), std::move(req._args[0]), std::move(req._args[1]));
}

future<reply> set::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    return redis::write_mutation(proxy, redis::make_simple(_schema, _key, std::move(_data)), cl, timeout, cs).then_wrapped([this] (auto f) {
        try {
            f.get();
        } catch (std::exception& e) {
            return reply_builder::build<error_tag>();
        }
        return reply_builder::build<ok_tag>();
    });
}

shared_ptr<abstract_command> mset::prepare(service::storage_proxy& proxy, request&& req)
{
    if (req._args_count < 2 || (req._args_count % 2 != 0)) {
        return unexpected::prepare(std::move(req._command), std::move(bytes {msg_syntax_err}));
    }
    std::vector<std::pair<bytes, bytes>> data;
    for (size_t i = 0; i < req._args_count; i += 2) {
        data.emplace_back(std::move(std::pair<bytes, bytes>(std::move(req._args[i]), std::move(req._args[i + 1]))));
    }
    return seastar::make_shared<mset> (std::move(req._command), simple_objects_schema(proxy), std::move(data));
}

future<reply> mset::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    auto mutations = boost::copy_range<std::vector<seastar::lw_shared_ptr<redis_mutation<bytes>>>>(_datas | boost::adaptors::transformed([this] (auto& data) {
        return redis::make_simple(_schema, data.first, std::move(data.second));
    }));
    return redis::write_mutations(proxy, mutations, cl, timeout, cs).then_wrapped([this] (auto f) {
        try {
            f.get();
        } catch (std::exception& e) {
            return reply_builder::build<error_tag>();
        }
        return reply_builder::build<OK_tag>();
    });
}
}
}
