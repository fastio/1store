#pragma once
#include <vector>
#include "bytes.hh"
#include "redis/redis_command_code.hh"
namespace redis {
struct request {
    protocol_state _state;
    bytes _command;
    uint32_t _args_count;
    std::vector<bytes> _args;
};
}
