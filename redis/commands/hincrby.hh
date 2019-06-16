#pragma once
#include "redis/command_with_single_schema.hh"
#include "redis/request.hh"
class timeout_config;
namespace redis {
namespace commands {
class hincrby : public command_with_single_schema {
private:
    bytes _key;
    bytes _field;
    double _increment;
public:

    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    hincrby(bytes&& name, const schema_ptr schema, bytes&& key, bytes&& field, double increment) 
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
        , _field(std::move(field))
        , _increment(increment)
    {
    }
    ~hincrby() {}
    future<redis_message> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};
}
}
