#pragma once
#include "redis/command_with_single_schema.hh"
#include "redis/request.hh"

class timeout_config;
namespace redis {
namespace commands {
class set final : public command_with_single_schema {
public:
    enum flag_t : uint8_t {
        FLAG_SET_NO = 1 << 0,
        FLAG_SET_EX = 1 << 1,
        FLAG_SET_PX = 1 << 2,
        FLAG_SET_NX = 1 << 3,
        FLAG_SET_XX = 1 << 4,
    };
private:
    bytes _key;
    bytes _data;
    flag_t _flag;
public:

    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, request&& req);
    set(bytes&& name, const schema_ptr schema, bytes&& key, bytes&& data, const gc_clock::duration ttl) 
        : command_with_single_schema(std::move(name), schema, ttl)
        , _key(std::move(key))
        , _data(std::move(data))
    {
    }
    set(bytes&& name, const schema_ptr schema, bytes&& key, bytes&& data) 
        : command_with_single_schema(std::move(name), schema) 
        , _key(std::move(key))
        , _data(std::move(data))
    {
    }
    ~set() {}
    future<reply> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
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
    future<reply> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};
}
}
