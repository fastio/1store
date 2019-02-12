#pragma once
#include "redis/abstract_command.hh"
#include "redis/request.hh"
namespace redis {
namespace commands {
class set final : public abstract_command {
public:
    enum flag_t : uint8_t {
        FLAG_SET_NO = 1 << 0,
        FLAG_SET_EX = 1 << 1,
        FLAG_SET_PX = 1 << 2,
        FLAG_SET_NX = 1 << 3,
        FLAG_SET_XX = 1 << 4,
    };
private:
    bytes _key;
    bytes _data;
    long _ttl;
    flag_t _flag;
public:

    static shared_ptr<abstract_command> prepare(request&& req);
    set(bytes&& name, bytes&& key, bytes&& data, long ttl, flag_t flag) 
        : abstract_command(std::move(name))
        , _key(std::move(key))
        , _data(std::move(data))
        , _ttl(ttl)
        , _flag(flag)
    {
    }
    set(bytes&& name, bytes&& key, bytes&& data) 
        : abstract_command(std::move(name))
        , _key(std::move(key))
        , _data(std::move(data))
        , _ttl(0)
        , _flag(flag_t::FLAG_SET_NO)
    {
    }
    ~set() {}
    future<reply> execute() override;
};
}
}
