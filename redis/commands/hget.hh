#pragma once
#include "redis/command_with_single_schema.hh"
#include "redis/request.hh"

namespace query {
class result;
}

class timeout_config;
namespace redis {
namespace commands {
class hget : public command_with_single_schema {
protected:
    bytes _key;
    std::vector<bytes> _map_keys;
    bool _multi = false;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req, bool multi);
    hget(bytes&& name, const schema_ptr schema, bytes&& key, std::vector<bytes>&& map_keys, bool multi) 
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
        , _map_keys(std::move(map_keys))
        , _multi(multi)
    {
    }
    ~hget() {}
    future<reply> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};

}
}
