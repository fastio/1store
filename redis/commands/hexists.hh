#pragma once
#include "redis/request.hh"
#include "redis/commands/del.hh"
#include "redis/command_with_single_schema.hh"
class timeout_config;
namespace redis {
namespace commands {
class hexists : public command_with_single_schema {
protected:
    bytes _key;
    bytes _map_key;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    hexists(bytes&& name, const schema_ptr schema, bytes&& key, bytes&& map_key)
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
        , _map_key(std::move(map_key))
    {
    }
    ~hexists() {}
    future<redis_message> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};
}
}
