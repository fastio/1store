#pragma once
#include "redis/command_with_single_schema.hh"
#include "redis/request.hh"
#include "redis/prefetcher.hh"
namespace query {
class result;
}

class timeout_config;
namespace redis {

namespace commands {
class zrem : public command_with_single_schema {
protected:
    bytes _key;
    std::vector<bytes> _members;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    zrem(bytes&& name, const schema_ptr schema, bytes&& key, std::vector<bytes>&& members) 
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
        , _members(std::move(members))
    {
    }
    ~zrem() {}
    future<redis_message> execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs) override;
};

class zremrangebyrank : public command_with_single_schema {
protected: 
    bytes _key;
    long _begin;
    long _end;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    zremrangebyrank(bytes&& name, const schema_ptr schema, bytes&& key, long begin, long end)
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
        , _begin(begin)
        , _end(end)
    {
    }
    ~zremrangebyrank() {}
    future<redis_message> execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs) override; 
};

class zremrangebyscore : public command_with_single_schema {
protected: 
    bytes _key;
    double _min;
    double _max;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    zremrangebyscore(bytes&& name, const schema_ptr schema, bytes&& key, double min, double max)
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
        , _min(min)
        , _max(max)
    {
    }
    ~zremrangebyscore() {}
    future<redis_message> execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs) override; 
};

}
}
