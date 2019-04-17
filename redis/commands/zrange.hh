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
class zrange : public command_with_single_schema {
protected:
    bytes _key;
    long _begin;
    long _end;
    bool _with_scores = false;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    zrange(bytes&& name, const schema_ptr schema, bytes&& key, long begin, long end, bool with_scores) 
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
        , _begin(begin)
        , _end(end)
        , _with_scores(with_scores)
    {
    }
    ~zrange() {}
    future<redis_message> execute_impl(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs, bool reversed);
    future<redis_message> execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs) override {
        return execute_impl(proxy, cl, now, tc, cs, false); 
    }
};

class zrevrange : public zrange {
public: 
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    zrevrange(bytes&& name, const schema_ptr schema, bytes&& key, long begin, long end, bool with_scores)
        : zrange(std::move(name), schema, std::move(key), begin, end, with_scores)
    {
    }
    ~zrevrange() {}
    future<redis_message> execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs) override {
        return execute_impl(proxy, cl, now, tc, cs, true);
    }
};

}
}
