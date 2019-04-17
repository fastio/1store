#pragma once
#include "redis/request.hh"
#include "redis/commands/del.hh"
#include "redis/command_with_multi_schemas.hh"
class timeout_config;
namespace redis {
namespace commands {
class exists final : public del {
public:
    static shared_ptr<abstract_command> prepare(service::storage_proxy& proxy, const service::client_state& cs, request&& req);
    exists(bytes&& name, std::vector<schema_ptr> schemas, bytes&& key)
        : del(std::move(name), std::move(schemas), std::move(key))
    {
    }
    ~exists() {}
    future<redis_message> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};
}
}
