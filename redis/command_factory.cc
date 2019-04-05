#include "redis/command_factory.hh"
#include "service/storage_proxy.hh"
#include "redis/commands/unexpected.hh"
#include "redis/commands/set.hh"
#include "redis/commands/get.hh"
#include "redis/commands/del.hh"
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
#include "redis/commands/lset.hh"
#include "redis/commands/ltrim.hh"
#include "redis/commands/hset.hh"
#include "redis/commands/hget.hh"
#include "redis/commands/hdel.hh"
#include "redis/commands/hexists.hh"
#include "redis/commands/sset.hh"
#include "redis/commands/smembers.hh"
#include "redis/commands/srem.hh"
#include "redis/commands/zadd.hh"
#include "redis/commands/zscore.hh"
#include "redis/commands/zincrby.hh"
#include "redis/commands/zcard.hh"
#include "redis/commands/zcount.hh"
#include "redis/commands/zrange.hh"
#include "redis/commands/zrangebyscore.hh"
#include "redis/commands/zrank.hh"
#include "redis/commands/zrem.hh"
#include "redis/commands/expire.hh"

namespace redis {
shared_ptr<abstract_command> command_factory::create(service::storage_proxy& proxy, request&& req)
{
    static thread_local std::unordered_map<bytes, std::function<shared_ptr<abstract_command> (service::storage_proxy& proxy, request&& req)>> _commands = {
    { "set",  [] (service::storage_proxy& proxy, request&& req) { return commands::set::prepare(proxy, std::move(req)); } }, 
    { "setnx",  [] (service::storage_proxy& proxy, request&& req) { return commands::setnx::prepare(proxy, std::move(req)); } }, 
    { "setex",  [] (service::storage_proxy& proxy, request&& req) { return commands::setex::prepare(proxy, std::move(req)); } }, 
    { "mset",  [] (service::storage_proxy& proxy, request&& req) { return commands::mset::prepare(proxy, std::move(req)); } }, 
    { "get",  [] (service::storage_proxy& proxy, request&& req) { return commands::get::prepare(proxy, std::move(req)); } }, 
    { "getset",  [] (service::storage_proxy& proxy, request&& req) { return commands::getset::prepare(proxy, std::move(req)); } }, 
    { "mget",  [] (service::storage_proxy& proxy, request&& req) { return commands::mget::prepare(proxy, std::move(req)); } }, 
    { "del",  [] (service::storage_proxy& proxy, request&& req) { return commands::del::prepare(proxy, std::move(req)); } }, 
    { "exists",  [] (service::storage_proxy& proxy, request&& req) { return commands::exists::prepare(proxy, std::move(req)); } }, 
    { "expire",  [] (service::storage_proxy& proxy, request&& req) { return commands::expire::prepare(proxy, std::move(req)); } }, 
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
    { "lset",  [] (service::storage_proxy& proxy, request&& req) { return commands::lset::prepare(proxy, std::move(req)); } }, 
    { "ltrim",  [] (service::storage_proxy& proxy, request&& req) { return commands::ltrim::prepare(proxy, std::move(req)); } }, 
    { "hset",  [] (service::storage_proxy& proxy, request&& req) { return commands::hset::prepare(proxy, std::move(req), false); } }, 
    { "hmset",  [] (service::storage_proxy& proxy, request&& req) { return commands::hset::prepare(proxy, std::move(req), true); } }, 
    { "hget",  [] (service::storage_proxy& proxy, request&& req) { return commands::hget::prepare(proxy, std::move(req), false); } }, 
    { "hmget",  [] (service::storage_proxy& proxy, request&& req) { return commands::hget::prepare(proxy, std::move(req), true); } }, 
    { "hdel",  [] (service::storage_proxy& proxy, request&& req) { return commands::hdel::prepare(proxy, std::move(req)); } }, 
    { "hexists",  [] (service::storage_proxy& proxy, request&& req) { return commands::hexists::prepare(proxy, std::move(req)); } }, 
    { "hkeys",  [] (service::storage_proxy& proxy, request&& req) { return commands::hkeys::prepare(proxy, std::move(req)); } }, 
    { "hvals",  [] (service::storage_proxy& proxy, request&& req) { return commands::hvals::prepare(proxy, std::move(req)); } }, 
    { "hgetall",  [] (service::storage_proxy& proxy, request&& req) { return commands::hgetall::prepare(proxy, std::move(req)); } }, 
    { "sadd",  [] (service::storage_proxy& proxy, request&& req) { return commands::sset::prepare(proxy, std::move(req)); } }, 
    { "smembers",  [] (service::storage_proxy& proxy, request&& req) { return commands::smembers::prepare(proxy, std::move(req)); } }, 
    { "srem",  [] (service::storage_proxy& proxy, request&& req) { return commands::srem::prepare(proxy, std::move(req)); } }, 
    { "zadd",  [] (service::storage_proxy& proxy, request&& req) { return commands::zadd::prepare(proxy, std::move(req)); } }, 
    { "zscore",  [] (service::storage_proxy& proxy, request&& req) { return commands::zscore::prepare(proxy, std::move(req)); } }, 
    { "zincrby",  [] (service::storage_proxy& proxy, request&& req) { return commands::zincrby::prepare(proxy, std::move(req)); } }, 
    { "zcount",  [] (service::storage_proxy& proxy, request&& req) { return commands::zcount::prepare(proxy, std::move(req)); } }, 
    { "zcard",  [] (service::storage_proxy& proxy, request&& req) { return commands::zcard::prepare(proxy, std::move(req)); } }, 
    { "zrange",  [] (service::storage_proxy& proxy, request&& req) { return commands::zrange::prepare(proxy, std::move(req)); } }, 
    { "zrevrange",  [] (service::storage_proxy& proxy, request&& req) { return commands::zrevrange::prepare(proxy, std::move(req)); } }, 
    { "zrangebyscore",  [] (service::storage_proxy& proxy, request&& req) { return commands::zrangebyscore::prepare(proxy, std::move(req)); } }, 
    { "zrevrangebyscore",  [] (service::storage_proxy& proxy, request&& req) { return commands::zrevrangebyscore::prepare(proxy, std::move(req)); } }, 
    { "zrank",  [] (service::storage_proxy& proxy, request&& req) { return commands::zrank::prepare(proxy, std::move(req)); } }, 
    { "zrevrank",  [] (service::storage_proxy& proxy, request&& req) { return commands::zrevrank::prepare(proxy, std::move(req)); } }, 
    { "zrem",  [] (service::storage_proxy& proxy, request&& req) { return commands::zrem::prepare(proxy, std::move(req)); } }, 
    { "zremrangebyrank",  [] (service::storage_proxy& proxy, request&& req) { return commands::zremrangebyrank::prepare(proxy, std::move(req)); } }, 
    { "zremrangebyscore",  [] (service::storage_proxy& proxy, request&& req) { return commands::zremrangebyscore::prepare(proxy, std::move(req)); } }, 
    };
    std::transform(req._command.begin(), req._command.end(), req._command.begin(), ::tolower);
    auto&& command = _commands.find(req._command);
    if (command != _commands.end()) {
        return (command->second)(proxy, std::move(req));
    }
    return commands::unexpected::prepare(std::move(req._command));
}

}
