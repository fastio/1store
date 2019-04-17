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
class zcount : public command_with_single_schema {
protected:
    bytes _key;
    double _min;
    double _max;
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    zcount(bytes&& name, const schema_ptr schema, bytes&& key, double min, double max) 
        : command_with_single_schema(std::move(name), schema)
        , _key(std::move(key))
        , _min(min)
        , _max(max)
    {
    }
    ~zcount() {}
    future<redis_message> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};

}
}
