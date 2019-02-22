#pragma once
#include "redis/command_with_single_schema.hh"
#include "redis/request.hh"

namespace query {
class result;
}

class timeout_config;
namespace redis {
namespace commands {
class get : public command_with_single_schema {
protected:
    bytes _key;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    get(bytes&& name, const schema_ptr schema, bytes&& key) 
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
    {
    }
    ~get() {}
    future<reply> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};

class getset : public get {
    bytes _data;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    getset(bytes&& name, const schema_ptr schema, bytes&& key, bytes&& data) 
        : get(std::move(name), schema, std::move(key))
        , _data(std::move(data))
    {
    }
    ~getset() {}
    future<reply> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};

}
}
