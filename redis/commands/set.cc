#include "redis/commands/set.hh"
#include "redis/commands/unexpected.hh"
#include "seastar/core/shared_ptr.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "db/system_keyspace.hh"
#include "types.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "mutation.hh"
#include "timeout_config.hh"
#include "redis/redis_mutation.hh"
#include "redis/prefetcher.hh"
namespace redis {

namespace commands {

shared_ptr<abstract_command> set::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count != 2) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 2, req._args_count);
    }
    return seastar::make_shared<set> (std::move(req._command), simple_objects_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), std::move(req._args[1]));
}
/*
shared_ptr<abstract_command> setnx::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count != 2) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 2, req._args_count);
    }
    return seastar::make_shared<setnx> (std::move(req._command), simple_objects_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), std::move(req._args[1]));
}
*/
shared_ptr<abstract_command> setex::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count != 3) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 3, req._args_count);
    }
    long ttl = 0;
    try {
        ttl = bytes2long(req._args[1]);
    } catch(std::exception&) {
        return unexpected::make_exception(std::move(req._command), sstring("-ERR value is not an integer or out of range"));
    }
    //std::chrono::seconds(ttl));
    return seastar::make_shared<setex> (std::move(req._command), simple_objects_schema(proxy, cs.get_keyspace()), std::move(req._args[0]), std::move(req._args[2]), ttl);
}

future<redis_message> set::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    return redis::write_mutation(proxy, redis::make_simple(_schema, _key, std::move(_data), _ttl), cl, timeout, cs).then_wrapped([this] (auto f) {
        try {
            f.get();
        } catch (std::exception& e) {
            return redis_message::err();
        }
        return redis_message::ok();
    });
}

/*
future<redis_message> setnx::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    return redis::exists(proxy, _schema, _key, cl, timeout, cs).then([this, &proxy, cl, timeout, &cs] (auto exists) {
        if (!exists) {
            return redis::write_mutation(proxy, redis::make_simple(_schema, _key, std::move(_data)), cl, timeout, cs).then_wrapped([this] (auto f) {
                try {
                    f.get();
                } catch (std::exception& e) {
                    return redis_message::err();
                }
                return redis_message::ok();
            });
        }
        return redis_message::err();
    });
}
*/
template<typename Type>
shared_ptr<abstract_command> prepare_impl(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    if (req._args_count < 2 || (req._args_count % 2 != 0)) {
        return unexpected::make_wrong_arguments_exception(std::move(req._command), 2, req._args_count);
    }
    std::vector<std::pair<bytes, bytes>> data;
    for (size_t i = 0; i < req._args_count; i += 2) {
        data.emplace_back(std::move(std::pair<bytes, bytes>(std::move(req._args[i]), std::move(req._args[i + 1]))));
    }
    return seastar::make_shared<Type> (std::move(req._command), simple_objects_schema(proxy, cs.get_keyspace()), std::move(data));
}

shared_ptr<abstract_command> mset::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    return prepare_impl<mset>(proxy, cs, std::move(req));
}

/*shared_ptr<abstract_command> msetnx::prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    return prepare_impl<msetnx>(proxy, cs, std::move(req));
}
*/
future<redis_message> mset::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    auto mutations = boost::copy_range<std::vector<seastar::lw_shared_ptr<redis_mutation<bytes>>>>(_data | boost::adaptors::transformed([this] (auto& data) {
        return redis::make_simple(_schema, data.first, std::move(data.second));
    }));
    return redis::write_mutations(proxy, mutations, cl, timeout, cs).then_wrapped([this] (auto f) {
        try {
            f.get();
        } catch (std::exception& e) {
            return redis_message::err();
        }
        return redis_message::ok();
    });
}
/*
future<redis_message> msetnx::execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs)
{
    auto timeout = now + tc.write_timeout;
    return do_with(bool { false }, [this, &proxy, cl, timeout, &cs] (auto& exists_at_least_one) {
        return parallel_for_each(_data.begin(), _data.end(), [this, &proxy, timeout, cl, &cs, &exists_at_least_one] (auto& data) {
            return redis::exists(proxy, _schema, std::get<0>(data), cl, timeout, cs).then([this, &proxy, cl, timeout, &cs, &data, &exists_at_least_one] (auto exists) {
                exists_at_least_one |= exists;
            });
        }).then([this, &proxy, timeout, cl, &cs, &exists_at_least_one] {
            if (!exists_at_least_one) {
                auto mutations = boost::copy_range<std::vector<seastar::lw_shared_ptr<redis_mutation<bytes>>>>(_data | boost::adaptors::transformed([this] (auto& data) {
                    return redis::make_simple(_schema, data.first, std::move(data.second));
                }));
                return redis::write_mutations(proxy, mutations, cl, timeout, cs).then_wrapped([this] (auto f) {
                    try {
                        f.get();
                    } catch (std::exception& e) {
                        return redis_message::err();
                    }
                    return redis_message::one();
                });
            }
            return redis_message::zero();
        });
    });
}
*/
}
}
