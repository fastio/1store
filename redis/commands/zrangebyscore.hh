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
class zrangebyscore : public command_with_single_schema {
protected:
    bytes _key;
    double _min;
    double _max;
    bool _with_scores = false;
    long _offset = 0;
    long _count = -1;
    future<redis_message> execute_impl(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs, bool reversed);
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    zrangebyscore(bytes&& name, const schema_ptr schema, bytes&& key, double min, double max, bool with_scores, long offset, long count) 
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
        , _min(min)
        , _max(max)
        , _with_scores(with_scores)
        , _offset(offset)
        , _count(count)
    {
    }
    ~zrangebyscore() {}
    future<redis_message> execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs) override {
        return execute_impl(proxy, cl, now, tc, cs, false); 
    }
};

class zrevrangebyscore : public zrangebyscore {
public: 
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    zrevrangebyscore(bytes&& name, const schema_ptr schema, bytes&& key, double min, double max, bool with_scores, long offset, long count) 
        : zrangebyscore(std::move(name), schema, std::move(key), min, max, with_scores, offset, count)
    {
    }
    ~zrevrangebyscore() {}
    future<redis_message> execute(service::storage_proxy& proxy, db::consistency_level cl, db::timeout_clock::time_point now, const timeout_config& tc, service::client_state& cs) override {
        return execute_impl(proxy, cl, now, tc, cs, true);
    }
};

}
}
