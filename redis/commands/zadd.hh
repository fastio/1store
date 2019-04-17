#pragma once
#include "redis/command_with_single_schema.hh"
#include "redis/request.hh"
class timeout_config;
namespace redis {
namespace commands {
class zadd : public command_with_single_schema {
private:
    bytes _key;
    std::vector<std::pair<bytes, bytes>> _data;
public:

    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    zadd(bytes&& name, const schema_ptr schema, bytes&& key, std::vector<std::pair<bytes, bytes>>&& data) 
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
        , _data(std::move(data))
    {
    }
    ~zadd() {}
    future<redis_message> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};
}
}
