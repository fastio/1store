#include "service/client_state.hh"
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
#include "redis/commands/hincrby.hh"
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
#include "redis/commands/cluster_slots.hh"
#include "redis/commands/spop.hh"
#include "redis/commands/srandmember.hh"
#include "redis/commands/scard.hh"
#include "log.hh"
namespace redis {
static logging::logger logging("command_factory");
shared_ptr<abstract_command> command_factory::create(service::storage_proxy& proxy, const service::client_state& cs, request&& req)
{
    static thread_local std::unordered_map<bytes, std::function<shared_ptr<abstract_command> (service::storage_proxy& proxy, const service::client_state& cs, request&& req)>> _commands = {
    { "set",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::set::prepare(proxy, cs, std::move(req)); } }, 
//  { "setnx",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::setnx::prepare(proxy, cs, std::move(req)); } }, 
    { "setex",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::setex::prepare(proxy, cs, std::move(req)); } }, 
    { "mset",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::mset::prepare(proxy, cs, std::move(req)); } }, 
//  { "msetnx",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::msetnx::prepare(proxy, cs, std::move(req)); } }, 
    { "get",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::get::prepare(proxy, cs, std::move(req)); } }, 
//  { "getset",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::getset::prepare(proxy, cs, std::move(req)); } }, 
    { "mget",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::mget::prepare(proxy, cs, std::move(req)); } }, 
    { "del",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::del::prepare(proxy, cs, std::move(req)); } }, 
    { "exists",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::exists::prepare(proxy, cs, std::move(req)); } }, 
    { "expire",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::expire::prepare(proxy, cs, std::move(req)); } }, 
    { "persist",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::persist::prepare(proxy, cs, std::move(req)); } }, 
    { "strlen",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::strlen::prepare(proxy, cs, std::move(req)); } }, 
    { "append",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::append::prepare(proxy, cs, std::move(req)); } }, 
    { "incr",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::counter::prepare(proxy, cs, commands::counter::incr_tag {}, std::move(req)); } }, 
    { "decr",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::counter::prepare(proxy, cs, commands::counter::decr_tag {}, std::move(req)); } }, 
    { "incrby",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::counter::prepare(proxy, cs, commands::counter::incrby_tag {}, std::move(req)); } }, 
    { "decrby",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::counter::prepare(proxy, cs, commands::counter::decrby_tag {}, std::move(req)); } }, 
    { "lpush",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::lpush::prepare(proxy, cs, std::move(req)); } }, 
    { "lpushx",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::lpushx::prepare(proxy, cs, std::move(req)); } }, 
    { "rpush",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::rpush::prepare(proxy, cs, std::move(req)); } }, 
    { "rpushx",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::rpushx::prepare(proxy, cs, std::move(req)); } }, 
    { "lpop",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::lpop::prepare(proxy, cs, std::move(req)); } }, 
    { "rpop",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::rpop::prepare(proxy, cs, std::move(req)); } }, 
    { "lrange",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::lrange::prepare(proxy, cs, std::move(req)); } }, 
    { "llen",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::llen::prepare(proxy, cs, std::move(req)); } }, 
    { "lindex",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::lindex::prepare(proxy, cs, std::move(req)); } }, 
    { "lrem",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::lrem::prepare(proxy, cs, std::move(req)); } }, 
    { "lset",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::lset::prepare(proxy, cs, std::move(req)); } }, 
    { "ltrim",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::ltrim::prepare(proxy, cs, std::move(req)); } }, 
    { "hset",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::hset::prepare(proxy, cs, std::move(req), false); } }, 
    { "hmset",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::hset::prepare(proxy, cs, std::move(req), true); } }, 
    { "hget",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::hget::prepare(proxy, cs, std::move(req), false); } }, 
    { "hmget",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::hget::prepare(proxy, cs, std::move(req), true); } }, 
    { "hdel",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::hdel::prepare(proxy, cs, std::move(req)); } }, 
    { "hincrby",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::hincrby::prepare(proxy, cs, std::move(req)); } }, 
    { "hexists",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::hexists::prepare(proxy, cs, std::move(req)); } }, 
    { "hkeys",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::hkeys::prepare(proxy, cs, std::move(req)); } }, 
    { "hvals",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::hvals::prepare(proxy, cs, std::move(req)); } }, 
    { "hgetall",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::hgetall::prepare(proxy, cs, std::move(req)); } }, 
    { "sadd",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::sset::prepare(proxy, cs, std::move(req)); } }, 
    { "smembers",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::smembers::prepare(proxy, cs, std::move(req)); } }, 
    { "spop",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::spop::prepare(proxy, cs, std::move(req)); } }, 
    { "scard",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::scard::prepare(proxy, cs, std::move(req)); } }, 
    { "srandmember",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::srandmember::prepare(proxy, cs, std::move(req)); } }, 
    { "srem",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::srem::prepare(proxy, cs, std::move(req)); } }, 
    { "zadd",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::zadd::prepare(proxy, cs, std::move(req)); } }, 
    { "zscore",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::zscore::prepare(proxy, cs, std::move(req)); } }, 
    { "zincrby",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::zincrby::prepare(proxy, cs, std::move(req)); } }, 
    { "zcount",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::zcount::prepare(proxy, cs, std::move(req)); } }, 
    { "zcard",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::zcard::prepare(proxy, cs, std::move(req)); } }, 
    { "zrange",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::zrange::prepare(proxy, cs, std::move(req)); } }, 
    { "zrevrange",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::zrevrange::prepare(proxy, cs, std::move(req)); } }, 
    { "zrangebyscore",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::zrangebyscore::prepare(proxy, cs, std::move(req)); } }, 
    { "zrevrangebyscore",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::zrevrangebyscore::prepare(proxy, cs, std::move(req)); } }, 
    { "zrank",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::zrank::prepare(proxy, cs, std::move(req)); } }, 
    { "zrevrank",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::zrevrank::prepare(proxy, cs, std::move(req)); } }, 
    { "zrem",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::zrem::prepare(proxy, cs, std::move(req)); } }, 
    { "zremrangebyrank",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::zremrangebyrank::prepare(proxy, cs, std::move(req)); } }, 
    { "zremrangebyscore",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::zremrangebyscore::prepare(proxy, cs, std::move(req)); } }, 
    { "cluster",  [] (service::storage_proxy& proxy, const service::client_state& cs, request&& req) { return commands::cluster_slots::prepare(proxy, cs, std::move(req)); } }, 
    };
    auto&& command = _commands.find(req._command);
    if (command != _commands.end()) {
        return (command->second)(proxy, cs, std::move(req));
    }
    auto& b = req._command;
    logging.error("unkown command = {}", sstring(reinterpret_cast<const char*>(b.data()), b.size()));
    return commands::unexpected::prepare(std::move(req._command));
}

}
