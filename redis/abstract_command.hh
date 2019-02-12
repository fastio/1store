#pragma once
#include "bytes.hh"
#include "seastar/core/future.hh"
#include "redis/request.hh"
#include "redis/reply.hh"
namespace redis {

using namespace seastar;

class abstract_command : public enable_shared_from_this<abstract_command> {
protected:
    bytes _name;
public:
    abstract_command(bytes&& name) : _name(std::move(name)) {}
    virtual ~abstract_command() {};
    virtual future<reply> execute() = 0;
    const bytes& name() const { return _name; }
};

}
