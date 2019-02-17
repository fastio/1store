#include "redis/command_factory.hh"
#include "redis/commands/set.hh"
#include "redis/commands/get.hh"
#include "redis/commands/del.hh"
#include "redis/commands/unexpected.hh"
#include "redis/commands/exists.hh"
#include "redis/commands/strlen.hh"
#include "redis/commands/append.hh"
#include "redis/commands/counter.hh"
namespace redis {
shared_ptr<abstract_command> command_factory::create(request&& req)
{
    static thread_local std::unordered_map<bytes, std::function<shared_ptr<abstract_command> (request&& req)>> _commands = {
    { "set",  [] (request&& req) { return commands::set::prepare(std::move(req)); } }, 
    { "get",  [] (request&& req) { return commands::get::prepare(std::move(req)); } }, 
    { "del",  [] (request&& req) { return commands::del::prepare(std::move(req)); } }, 
    { "exists",  [] (request&& req) { return commands::exists::prepare(std::move(req)); } }, 
    { "strlen",  [] (request&& req) { return commands::strlen::prepare(std::move(req)); } }, 
    { "append",  [] (request&& req) { return commands::append::prepare(std::move(req)); } }, 
    { "incr",  [] (request&& req) { return commands::counter::prepare(commands::counter::incr_tag {}, std::move(req)); } }, 
    { "decr",  [] (request&& req) { return commands::counter::prepare(commands::counter::decr_tag {}, std::move(req)); } }, 
    { "incrby",  [] (request&& req) { return commands::counter::prepare(commands::counter::incrby_tag {}, std::move(req)); } }, 
    { "decrby",  [] (request&& req) { return commands::counter::prepare(commands::counter::decrby_tag {}, std::move(req)); } }, 
    };
    std::transform(req._command.begin(), req._command.end(), req._command.begin(), ::tolower);
    auto&& command = _commands.find(req._command);
    if (command != _commands.end()) {
        return (command->second)(std::move(req));
    }
    return commands::unexpected::prepare(std::move(req._command));
}

}
