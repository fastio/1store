#pragma once
#include "redis/command_with_single_schema.hh"
#include "redis/request.hh"

namespace service {
class storage_proxy;
}
class timeout_config;
namespace redis {
namespace commands {
class llen : public command_with_single_schema {
protected:
    bytes _key;
public:
    llen(bytes&& name, const schema_ptr schema, bytes&& key) 
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
    {
    }
    ~llen() {}
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    virtual future<redis_message> execute(service::storage_proxy&,
        db::consistency_level,
        db::timeout_clock::time_point,
        const timeout_config& tc,
        service::client_state& cs
        ) override;
};

}
}
