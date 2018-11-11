/*
* Pedis is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* You may obtain a copy of the License at
*
*     http://www.gnu.org/licenses
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
*  Copyright (c) 2016-2026, Peng Jian, pengjian.uest@gmail.com. All rights reserved.
*
*/
#include "server.hh"
#include "core/execution_stage.hh"
#include "utils/bytes.hh"
namespace redis {

distributed<server> _server;

static thread_local auto reqeust_process_stage = seastar::make_execution_stage("request_proccessor", &server::connection::handle);
static thread_local redis_service _redis;
static inline redis_service& redis() {
    return _redis;
}
static thread_local std::unordered_map<bytes, std::function<future<scattered_message_ptr>(request_wrapper& req)>> _commands = {
    { "set",  [] (request_wrapper& req) { return redis().set(req); } }, 
    { "mset", [] (request_wrapper& req) { return redis().mset(req); } }, 
    { "get",  [] (request_wrapper& req) { return redis().get(req); } }, 
    { "del",  [] (request_wrapper& req) { return redis().del(req); } }, 
    { "ping", [] (request_wrapper& req) { return redis().ping(req); } }, 
    { "incr", [] (request_wrapper& req) { return redis().incr(req); } }, 
    { "decr", [] (request_wrapper& req) { return redis().decr(req); } }, 
    { "incrby", [] (request_wrapper& req) { return redis().incrby(req); } }, 
    { "decrby", [] (request_wrapper& req) { return redis().decrby(req); } }, 
    { "mget", [] (request_wrapper& req) { return redis().mget(req); } }, 
    { "command", [] (request_wrapper& req) { return redis().command(req); } }, 
    { "exists", [] (request_wrapper& req) { return redis().exists(req); } }, 
    { "append", [] (request_wrapper& req) { return redis().append(req); } }, 
    { "strlen", [] (request_wrapper& req) { return redis().strlen(req); } }, 
    { "lpush", [] (request_wrapper& req) { return redis().lpush(req); } }, 
    { "lpushx", [] (request_wrapper& req) { return redis().lpushx(req); } }, 
    { "lpop", [] (request_wrapper& req) { return redis().lpop(req); } }, 
    { "llen", [] (request_wrapper& req) { return redis().llen(req); } }, 
    { "lindex", [] (request_wrapper& req) { return redis().lindex(req); } }, 
    { "linsert", [] (request_wrapper& req) { return redis().linsert(req); } }, 
    { "lrange", [] (request_wrapper& req) { return redis().lrange(req); } }, 
    { "lset", [] (request_wrapper& req) { return redis().lset(req); } }, 
    { "rpush", [] (request_wrapper& req) { return redis().rpush(req); } }, 
    { "rpushx", [] (request_wrapper& req) { return redis().rpushx(req); } }, 
    { "rpop", [] (request_wrapper& req) { return redis().rpop(req); } }, 
    { "lrem", [] (request_wrapper& req) { return redis().lrem(req); } }, 
    { "ltrim", [] (request_wrapper& req) { return redis().ltrim(req); } }, 
    { "hset", [] (request_wrapper& req) { return redis().hset(req); } }, 
    { "hmset", [] (request_wrapper& req) { return redis().hmset(req); } }, 
    { "hdel", [] (request_wrapper& req) { return redis().hdel(req); } }, 
    { "hget", [] (request_wrapper& req) { return redis().hget(req); } }, 
    { "hlen", [] (request_wrapper& req) { return redis().hlen(req); } }, 
    { "hexists", [] (request_wrapper& req) { return redis().hexists(req); } }, 
    { "hstrlen", [] (request_wrapper& req) { return redis().hstrlen(req); } }, 
    { "hincrby", [] (request_wrapper& req) { return redis().hincrby(req); } }, 
    { "hincrbyfloat", [] (request_wrapper& req) { return redis().hincrbyfloat(req); } }, 
    { "hkeys", [] (request_wrapper& req) { return redis().hgetall_keys(req); } }, 
    { "hvals", [] (request_wrapper& req) { return redis().hgetall_values(req); } }, 
    { "hmget", [] (request_wrapper& req) { return redis().hmget(req); } }, 
    { "hgetall", [] (request_wrapper& req) { return redis().hgetall(req); } }, 
    { "sadd", [] (request_wrapper& req) { return redis().sadd(req); } }, 
    { "scard", [] (request_wrapper& req) { return redis().scard(req); } }, 
    { "sismember", [] (request_wrapper& req) { return redis().sismember(req); } }, 
    { "smembers", [] (request_wrapper& req) { return redis().smembers(req); } }, 
    { "srandmember", [] (request_wrapper& req) { return redis().srandmember(req); } }, 
    { "srem", [] (request_wrapper& req) { return redis().srem(req); } }, 
    { "sdiff", [] (request_wrapper& req) { return redis().sdiff(req); } }, 
    { "sdiffstore", [] (request_wrapper& req) { return redis().sdiff_store(req); } }, 
    { "sinter", [] (request_wrapper& req) { return redis().sinter(req); } }, 
    { "sinterstore", [] (request_wrapper& req) { return redis().sinter_store(req); } }, 
    { "sunion", [] (request_wrapper& req) { return redis().sunion(req); } }, 
    { "sunionstore", [] (request_wrapper& req) { return redis().sunion_store(req); } }, 
    { "smove", [] (request_wrapper& req) { return redis().smove(req); } }, 
    { "spop", [] (request_wrapper& req) { return redis().spop(req); } }, 
    { "type", [] (request_wrapper& req) { return redis().type(req); } }, 
    { "expire", [] (request_wrapper& req) { return redis().expire(req); } }, 
    { "pexpire", [] (request_wrapper& req) { return redis().pexpire(req); } }, 
    { "ttl", [] (request_wrapper& req) { return redis().ttl(req); } }, 
    { "pttl", [] (request_wrapper& req) { return redis().pttl(req); } }, 
    { "persist", [] (request_wrapper& req) { return redis().persist(req); } }, 
    { "zadd", [] (request_wrapper& req) { return redis().zadd(req); } }, 
    { "zrange", [] (request_wrapper& req) { return redis().zrange(req, false); } }, 
    { "zrevrange", [] (request_wrapper& req) { return redis().zrange(req, true); } }, 
    { "zrangebyscore", [] (request_wrapper& req) { return redis().zrangebyscore(req, false); } }, 
    { "zrevrangebyscore", [] (request_wrapper& req) { return redis().zrangebyscore(req, true); } }, 
    { "zrem", [] (request_wrapper& req) { return redis().zrem(req); } }, 
    { "zremrangebyscore", [] (request_wrapper& req) { return redis().zremrangebyscore(req); } }, 
    { "zremrangebyrank", [] (request_wrapper& req) { return redis().zremrangebyrank(req); } }, 
    { "zcard", [] (request_wrapper& req) { return redis().zcard(req); } }, 
    { "zcount", [] (request_wrapper& req) { return redis().zcount(req); } }, 
    { "zscore", [] (request_wrapper& req) { return redis().zscore(req); } }, 
    { "zincrby", [] (request_wrapper& req) { return redis().zincrby(req); } }, 
    { "zrank", [] (request_wrapper& req) { return redis().zrank(req, false); } }, 
    { "zrevrank", [] (request_wrapper& req) { return redis().zrank(req, true); } }, 
    { "zunionstore", [] (request_wrapper& req) { return redis().zunionstore(req); } }, 
    { "zinterstore", [] (request_wrapper& req) { return redis().zinterstore(req); } }, 
    { "select", [] (request_wrapper& req) { return redis().select(req); } }, 
    { "geoadd", [] (request_wrapper& req) { return redis().geoadd(req); } }, 
    { "geodist", [] (request_wrapper& req) { return redis().geodist(req); } }, 
    { "geopos", [] (request_wrapper& req) { return redis().geopos(req); } }, 
    { "geohash", [] (request_wrapper& req) { return redis().geohash(req); } }, 
    { "georadius", [] (request_wrapper& req) { return redis().georadius(req, false); } }, 
    { "georadiusbymember", [] (request_wrapper& req) { return redis().georadius(req, true); } }, 
    { "setbit", [] (request_wrapper& req) { return redis().setbit(req); } }, 
    { "getbit", [] (request_wrapper& req) { return redis().getbit(req); } }, 
    { "bitcount", [] (request_wrapper& req) { return redis().bitcount(req); } }, 
    { "pfadd", [] (request_wrapper& req) { return redis().pfadd(req); } }, 
    { "pfcount", [] (request_wrapper& req) { return redis().pfcount(req); } }, 
    { "pfmerge", [] (request_wrapper& req) { return redis().pfmerge(req); } }, 
};

void server::setup_metrics()
{
    namespace sm = seastar::metrics;
    _metrics.add_group("connections", {
        sm::make_counter("opened_total", [this] { return _stats._connections_total; }, sm::description("Total number of connections opened.")),
        sm::make_counter("current_total", [this] { return _stats._connections_current; }, sm::description("Total number of connections current opened.")),
    });

    _metrics.add_group("reqests", {
        sm::make_counter("served_total", [this] { return 0; }, sm::description("Total number of served requests.")),
        sm::make_counter("serving_total", [this] { return 0; }, sm::description("Total number of requests being serving.")),
        sm::make_counter("exception_total", [this] { return 0; }, sm::description("Total number of bad requests.")),
    });
}

future<scattered_message_ptr> server::connection::do_unexpect_request(request_wrapper& req)
{
    bytes msg {"-ERR Unknown or disabled command '"};
    msg.append(req._command.data(), req._command.size());
    static bytes tail {"'\r\n"};
    msg.append(tail.data(), tail.size());
    return reply_builder::build(msg);
}

future<scattered_message_ptr> server::connection::do_handle_one(request_wrapper& req)
{
    std::transform(req._command.begin(), req._command.end(), req._command.begin(), ::tolower);
    if (req._state == protocol_state::ok) {
        req.clear_temporary_containers();
        const auto& command = _commands.find(req._command);
        if (command != _commands.end()) {
            return (command->second)(req);
        }
    }
    else if (req._state == protocol_state::eof) {
        _done = true;
        return make_ready_future<scattered_message_ptr>();
    }
    return do_unexpect_request(req);
}

future<scattered_message_ptr> server::connection::handle()
{
    _parser.init();
    // NOTE: The command is handled sequentially. The parser will control the lifetime
    // of every parameters for command.
    return _in.consume(_parser).then([this] {
        return _replies.not_full().then([this] {
                return do_handle_one(_parser.request());
        });
    });
}

future<> server::connection::request()
{
    return do_until([this] { return _done; }, [this] {
        return _replies.not_full().then([this] {
            return reqeust_process_stage(this).then([this] (auto message) {
                _replies.push(reply_wrapper { std::move(message) });
                return make_ready_future<>();
            });
        });
    }).finally([this] {
        return _in.close();
    });
}

future<> server::connection::reply()
{
    return do_until([this] { return _done && _replies.empty(); }, [this] {
        return _replies.pop_eventually().then([this] (auto resp) {
            return _out.write(std::move(*resp._reply)).then([this] {
                return _out.flush();
            });
        });
    });
}

void server::start()
{
    listen_options lo;
    lo.reuse_address = true;
    _listener = engine().listen(make_ipv4_address({_port}), lo);
    keep_doing([this] {
       return _listener->accept().then([this] (connected_socket fd, socket_address addr) mutable {
           ++_stats._connections_total;
           ++_stats._connections_current;
           auto conn = make_lw_shared<connection>(std::move(fd), addr, _use_native_parser);
           do_until([conn] { return conn->_in.eof(); }, [conn] {
               return conn->process().then([conn] {
                   return conn->_out.flush();
               });
           }).finally([this, conn] {
               --_stats._connections_current;
               return conn->_out.close().finally([conn]{});
           });
       });
   }).or_terminate();
}
}
