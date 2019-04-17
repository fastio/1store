#pragma once
#include "redis/command_with_single_schema.hh"
#include "redis/request.hh"
#include "redis/prefetcher.hh"
namespace query {
class result;
}

class timeout_config;
namespace redis {

namespace commands {
class zrank : public command_with_single_schema {
protected:
    bytes _key;
    bytes _member;
    future<redis_message> execute_impl(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs, bool reversed);
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    zrank(bytes&& name, const schema_ptr schema, bytes&& key, bytes&& member) 
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
        , _member(std::move(member))
    {
    }
    ~zrank() {}
    future<redis_message> execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs) override {
        return execute_impl(proxy, cl, now, tc, cs, false);
    }
};

class zrevrank : public zrank {
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    zrevrank(bytes&& name, const schema_ptr schema, bytes&& key, bytes&& member)
        : zrank(std::move(name), schema, std::move(key), std::move(member))
    {
    }
    ~zrevrank() {}
    future<redis_message> execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs) override {
        return execute_impl(proxy, cl, now, tc, cs, true);
    }
};
}
}
