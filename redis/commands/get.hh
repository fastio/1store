#pragma once
#include "redis/abstract_command.hh"
#include "redis/request.hh"

namespace query {
class result;
}

class timeout_config;
namespace redis {
namespace commands {
class get : public abstract_command {
protected:
    bytes _key;
public:
    static shared_ptr<abstract_command> prepare(request&& req);
    get(bytes&& name, bytes&& key) 
        : abstract_command(std::move(name))
        , _key(std::move(key))
    {
    }
    ~get() {}
    future<reply> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config& tc, service::client_state& cs) override;
};
}
}
