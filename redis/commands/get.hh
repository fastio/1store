#pragma once
#include "redis/abstract_command.hh"
#include "redis/request.hh"
namespace redis {
namespace commands {
class get final : public abstract_command {
    bytes _key;
public:
    static shared_ptr<abstract_command> prepare(request&& req);
    get(bytes&& name, bytes&& key) 
        : abstract_command(std::move(name))
        , _key(std::move(key))
    {
    }
    ~get() {}
    future<reply> execute() override;
};
}
}
