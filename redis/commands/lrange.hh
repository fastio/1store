#pragma once
#include "redis/command_with_single_schema.hh"
#include "redis/request.hh"

namespace service {
class storage_proxy;
}
class timeout_config;
namespace redis {
namespace commands {
class lrange : public command_with_single_schema {
protected:
    bytes _key;
    long _begin;
    long _end;
public:
    lrange(bytes&& name, const schema_ptr schema, bytes&& key, long start, long end) 
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
        , _begin(start)
        , _end(end)
    {
    }
    ~lrange() {}
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
