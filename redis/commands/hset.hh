#pragma once
#include "redis/command_with_single_schema.hh"
#include "redis/request.hh"
#include <unordered_map>
class timeout_config;
namespace redis {
namespace commands {
class hset : public command_with_single_schema {
private:
    bytes _key;
    std::unordered_map<bytes, bytes> _data;
    bool _multi = false;
public:

    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req, bool multi);
    hset(bytes&& name, const schema_ptr schema, bytes&& key, std::unordered_map<bytes, bytes>&& data, bool multi) 
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
        , _data(std::move(data))
        , _multi(multi)
    {
    }
    ~hset() {}
    future<redis_message> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};
}
}
