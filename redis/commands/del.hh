#pragma once
#include "redis/command_with_multi_schemas.hh"
#include "redis/request.hh"

class timeout_config;
namespace redis {
namespace commands {
class del : public command_with_multi_schemas {
protected:
    bytes _key;
public:

    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    del(bytes&& name, std::vector<schema_ptr> schemas, bytes&& key) 
        : command_with_multi_schemas(std::move(name), std::move(schemas))
        , _key(std::move(key))
    {
    }
    ~del() {}
    future<redis_message> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};
}
}
