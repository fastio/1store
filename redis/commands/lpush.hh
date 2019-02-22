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
class push : public command_with_single_schema {
protected:
    bytes _key;
    std::vector<bytes> _datas;
public:
    push(bytes&& name, const schema_ptr schema, bytes&& key, std::vector<bytes>&& datas) 
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
        , _datas(std::move(datas))
    {
    }
    ~push() {}
    virtual future<bool> check_exists(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) = 0;
protected: 
    future<reply> do_execute(service::storage_proxy&,
        db::consistency_level,
        db::timeout_clock::time_point,
        const timeout_config& tc,
        service::client_state& cs,
        bool left
        );
};

class lpush : public push {
public: 
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    lpush(bytes&& name, const schema_ptr schema, bytes&& key, std::vector<bytes>&& datas) : push(std::move(name), schema, std::move(key), std::move(datas)) {}
    virtual future<bool> check_exists(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override
    {
        return make_ready_future<bool>(true);
    }
    future<reply> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};

class lpushx : public push {
public: 
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    lpushx(bytes&& name, const schema_ptr schema, bytes&& key, std::vector<bytes>&& datas) : push(std::move(name), schema, std::move(key), std::move(datas)) {}
    virtual future<bool> check_exists(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
    future<reply> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};

class rpush : public lpush {
public: 
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    rpush(bytes&& name, const schema_ptr schema, bytes&& key, std::vector<bytes>&& datas) : lpush(std::move(name), schema, std::move(key), std::move(datas)) {}
    future<reply> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};
class rpushx : public lpushx {
public: 
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    rpushx(bytes&& name, const schema_ptr schema, bytes&& key, std::vector<bytes>&& datas) : lpushx(std::move(name), schema, std::move(key), std::move(datas)) {}
    future<reply> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};
}
}
