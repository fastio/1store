#pragma once
#include "redis/abstract_command.hh"
#include "redis/request.hh"
#include "redis/commands/get.hh"

class timeout_config;
namespace redis {
namespace commands {
class append : public get {
protected:
    bytes _data;
public:
    static shared_ptr<abstract_command> prepare(request&& req);
    append(bytes&& name, bytes&& key, bytes&& data) : get(std::move(name), std::move(key)), _data(std::move(data))
    {
    }
    ~append() {}
    future<reply> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};
}
}
