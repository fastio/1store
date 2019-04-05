#pragma once
#include "redis/command_with_single_schema.hh"
#include "redis/request.hh"

class timeout_config;
namespace redis {
namespace commands {
class set : public command_with_single_schema {
protected:
    bytes _key;
    bytes _data;
    long _ttl = 0;
public:

    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    set(bytes&& name, const schema_ptr schema, bytes&& key, bytes&& data, long ttl) 
        : command_with_single_schema(std::move(name), schema) 
        , _key(std::move(key))
        , _data(std::move(data))
        , _ttl(ttl)
    {
    }
    set(bytes&& name, const schema_ptr schema, bytes&& key, bytes&& data)
        : set(std::move(name), schema, std::move(key), std::move(data), 0)
    {
    }
    ~set() {}
    future<redis_message> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};

class setnx : public set {
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    setnx(bytes&& name, const schema_ptr schema, bytes&& key, bytes&& data)
        : set(std::move(name), schema, std::move(key), std::move(data))
    {
    }
    ~setnx() {}
    future<redis_message> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};

class setex : public set {
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    setex(bytes&& name, const schema_ptr schema, bytes&& key, bytes&& data, long ttl)
        : set(std::move(name), schema, std::move(key), std::move(data), ttl)
    {
    }
    ~setex() {}
};

class mset : public command_with_single_schema {
private:
    std::vector<std::pair<bytes, bytes>> _datas;
public:
    mset(bytes&& name, const schema_ptr schema, std::vector<std::pair<bytes, bytes>>&& data)
        : command_with_single_schema(std::move(name), schema)
        , _datas(std::move(data))
    {
    }
    ~mset() {}
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    future<redis_message> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};
}
}
