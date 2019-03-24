#pragma once
#include "redis/abstract_command.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
#include "tracing/trace_state.hh"
#include "timeout_config.hh"
namespace redis {
namespace commands {
class unexpected : public abstract_command {
    bytes _exception_message;
    bytes default_exception_message() { 
        bytes m {"-ERR Unknown or disabled command\r\n'"};
        return m;
    }
public:
    unexpected(bytes&& name) : abstract_command(std::move(name)), _exception_message(default_exception_message()) {}
    unexpected(bytes&& name, bytes&& exception_message) : abstract_command(std::move(name)), _exception_message(std::move(exception_message)) {}
    virtual ~unexpected() {}
    static shared_ptr<abstract_command> prepare(bytes&& name) {
        return make_shared<unexpected>(std::move(name));
    }
    static shared_ptr<abstract_command> prepare(bytes&& name, bytes&& message) {
        return make_shared<unexpected>(std::move(name), std::move(message));
    }
    virtual future<redis_message> execute(service::storage_proxy&, db::consistency_level, db::timeout_clock::time_point, const timeout_config&, service::client_state&) override {
        return redis_message::make_exception(std::move(_exception_message)); 
    }
};
}
}
