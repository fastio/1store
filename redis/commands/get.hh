#pragma once
#include "redis/abstract_command.hh"
#include "redis/request.hh"

namespace query {
class result;
}

class timeout_config;
namespace redis {
namespace commands {
using query_result_type = foreign_ptr<lw_shared_ptr<query::result>>;
class get : public abstract_command {
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
protected:
    future<reply> do_execute_with_action(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point,
            const timeout_config& tc, service::client_state& cs, std::function<future<reply>(schema_ptr, query_result_type&&)>); 
};
}
}
