#pragma once
#include "redis/request.hh"
#include "redis/abstract_command.hh"
namespace query {
class result;
}

class timeout_config;
namespace redis {
namespace commands {
class cluster_slots : public abstract_command {
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    cluster_slots(bytes&& name) : abstract_command(std::move(name)) 
    {
    }
    ~cluster_slots() {}
    future<redis_message> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};

}
}
