#pragma once
#include "bytes.hh"
#include "seastar/core/shared_ptr.hh"
namespace redis {
using namespace seastar;
class abstract_command;
class request;
class command_factory {
public:
    command_factory() {}
    ~command_factory() {}
    static shared_ptr<abstract_command> create(request&&);
};
}
