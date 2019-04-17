#pragma once
#include "redis/command_with_single_schema.hh"
#include "redis/request.hh"
#include <vector>

namespace service {
class storage_proxy;
}
class timeout_config;
namespace redis {
namespace commands {
class pop : public command_with_single_schema {
protected:
    bytes _key;
public:
    pop(bytes&& name, const schema_ptr schema, bytes&& key) 
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
    {
    }
    ~pop() {}
protected: 
    future<redis_message> do_execute(service::storage_proxy&,
        db::consistency_level,
        db::timeout_clock::time_point,
        const timeout_config& tc,
        service::client_state& cs,
        bool left
        );
};

class lpop : public pop {
public: 
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    lpop(bytes&& name, const schema_ptr schema, bytes&& key) : pop(std::move(name), schema, std::move(key)) {}
    future<redis_message> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};

class rpop : public pop {
public: 
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    rpop(bytes&& name, const schema_ptr schema, bytes&& key) : pop(std::move(name), schema, std::move(key)) {}
    future<redis_message> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};
}
}
