#include "redis/command_factory.hh"
#include "service/storage_proxy.hh"
#include "redis/commands/set.hh"
#include "redis/commands/get.hh"
#include "redis/commands/del.hh"
#include "redis/commands/unexpected.hh"
#include "redis/commands/exists.hh"
#include "redis/commands/strlen.hh"
#include "redis/commands/append.hh"
#include "redis/commands/counter.hh"
#include "redis/commands/lpush.hh"
#include "redis/commands/lpop.hh"
#include "redis/commands/lrange.hh"
#include "redis/commands/llen.hh"
#include "redis/commands/lindex.hh"
#include "redis/commands/lrem.hh"
namespace redis {
shared_ptr<abstract_command> command_factory::create(service::storage_proxy& proxy, request&& req)
{
    static thread_local std::unordered_map<bytes, std::function<shared_ptr<abstract_command> (service::storage_proxy& proxy, request&& req)>> _commands = {
    { "set",  [] (service::storage_proxy& proxy, request&& req) { return commands::set::prepare(proxy, std::move(req)); } }, 
    { "get",  [] (service::storage_proxy& proxy, request&& req) { return commands::get::prepare(proxy, std::move(req)); } }, 
    { "getset",  [] (service::storage_proxy& proxy, request&& req) { return commands::getset::prepare(proxy, std::move(req)); } }, 
    { "del",  [] (service::storage_proxy& proxy, request&& req) { return commands::del::prepare(proxy, std::move(req)); } }, 
    { "exists",  [] (service::storage_proxy& proxy, request&& req) { return commands::exists::prepare(proxy, std::move(req)); } }, 
    { "strlen",  [] (service::storage_proxy& proxy, request&& req) { return commands::strlen::prepare(proxy, std::move(req)); } }, 
    { "append",  [] (service::storage_proxy& proxy, request&& req) { return commands::append::prepare(proxy, std::move(req)); } }, 
    { "incr",  [] (service::storage_proxy& proxy, request&& req) { return commands::counter::prepare(proxy, commands::counter::incr_tag {}, std::move(req)); } }, 
    { "decr",  [] (service::storage_proxy& proxy, request&& req) { return commands::counter::prepare(proxy, commands::counter::decr_tag {}, std::move(req)); } }, 
    { "incrby",  [] (service::storage_proxy& proxy, request&& req) { return commands::counter::prepare(proxy, commands::counter::incrby_tag {}, std::move(req)); } }, 
    { "decrby",  [] (service::storage_proxy& proxy, request&& req) { return commands::counter::prepare(proxy, commands::counter::decrby_tag {}, std::move(req)); } }, 
    { "lpush",  [] (service::storage_proxy& proxy, request&& req) { return commands::lpush::prepare(proxy, std::move(req)); } }, 
    { "lpushx",  [] (service::storage_proxy& proxy, request&& req) { return commands::lpushx::prepare(proxy, std::move(req)); } }, 
    { "rpush",  [] (service::storage_proxy& proxy, request&& req) { return commands::rpush::prepare(proxy, std::move(req)); } }, 
    { "rpushx",  [] (service::storage_proxy& proxy, request&& req) { return commands::rpushx::prepare(proxy, std::move(req)); } }, 
    { "lpop",  [] (service::storage_proxy& proxy, request&& req) { return commands::lpop::prepare(proxy, std::move(req)); } }, 
    { "rpop",  [] (service::storage_proxy& proxy, request&& req) { return commands::rpop::prepare(proxy, std::move(req)); } }, 
    { "lrange",  [] (service::storage_proxy& proxy, request&& req) { return commands::lrange::prepare(proxy, std::move(req)); } }, 
    { "llen",  [] (service::storage_proxy& proxy, request&& req) { return commands::llen::prepare(proxy, std::move(req)); } }, 
    { "lindex",  [] (service::storage_proxy& proxy, request&& req) { return commands::lindex::prepare(proxy, std::move(req)); } }, 
    { "lrem",  [] (service::storage_proxy& proxy, request&& req) { return commands::lrem::prepare(proxy, std::move(req)); } }, 
    };
    std::transform(req._command.begin(), req._command.end(), req._command.begin(), ::tolower);
    auto&& command = _commands.find(req._command);
    if (command != _commands.end()) {
        return (command->second)(proxy, std::move(req));
    }
    return commands::unexpected::prepare(std::move(req._command));
}

}
