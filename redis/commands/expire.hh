#pragma once
#include "redis/request.hh"
#include "redis/commands/del.hh"
#include "redis/command_with_multi_schemas.hh"
class timeout_config;
namespace redis {
namespace commands {
class expire : public command_with_multi_schemas {
private:
    bytes _key;
    long _ttl;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    expire(bytes&& name, std::vector<schema_ptr> schemas, bytes&& key, long ttl)
        : command_with_multi_schemas(std::move(name), std::move(schemas))
        , _key(std::move(key))
        , _ttl(ttl)
    {
    }
    ~expire() {}
    future<redis_message> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};

class persist : public expire {
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    persist(bytes&& name, std::vector<schema_ptr> schemas, bytes&& key ) : expire(std::move(name), schemas, std::move(key), 0) {}
    ~persist() {}

};
}
}
