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
*  Copyright (c) 2016-2026, Peng Jian, pstack@163.com. All rights reserved.
*
*/
#include "redis_protocol.hh"
#include "redis.hh"
#include <algorithm>
#include "reply_builder.hh"
#include "protocol_parser.hh"
namespace redis {

redis_protocol::redis_protocol(redis_service& redis, bool use_native_parser)
    : _redis(redis)
    //, _parser(use_native_parser ? make_native_protocol_parser() : make_ragel_protocol_parser())
    , _parser(make_native_protocol_parser())
{
}

void redis_protocol::prepare_request()
{
    _parser.request().clear_temporary_containers();
}

future<> redis_protocol::handle(input_stream<char>& in, output_stream<char>& out)
{
    _parser.init();
    // NOTE: The command is handled sequentially. The parser will control the lifetime
    // of every parameters for command.
    return in.consume(_parser).then([this, &in, &out] () -> future<> {
        auto& _req = _parser.request();
        switch (_req._state) {
            case protocol_state::eof:
            case protocol_state::error:
                return make_ready_future<>();

            case protocol_state::ok:
            {
                prepare_request();
                switch (_req._command) {
                case command_code::set:
                    return _redis.set(_req, std::ref(out));
                case command_code::mset:
                    return _redis.mset(_req, std::ref(out));
                case command_code::get:
                    return _redis.get(_req, std::ref(out));
                case command_code::del:
                    return _redis.del(_req, std::ref(out));
                case command_code::ping:
                    return out.write(msg_pong);
                case command_code::incr:
                    return _redis.incr(_req, std::ref(out));
                case command_code::decr:
                    return _redis.incr(_req, std::ref(out));
                case command_code::incrby:
                    return _redis.incrby(_req, std::ref(out));
                case command_code::decrby:
                    return _redis.decrby(_req, std::ref(out));
                case command_code::mget:
                    return _redis.mget(_req, out);
                case command_code::command:
                    return out.write(msg_ok);
                case command_code::exists:
                    return _redis.exists(_req, std::ref(out));
                case command_code::append:
                    return _redis.append(_req, std::ref(out));
                case command_code::strlen:
                    return _redis.strlen(_req, std::ref(out));
                case command_code::lpush:
                    return _redis.lpush(_req, std::ref(out));
                case command_code::lpushx:
                    return _redis.lpushx(_req, std::ref(out));
                case command_code::lpop:
                    return _redis.lpop(_req, std::ref(out));
                case command_code::llen:
                    return _redis.llen(_req, std::ref(out));
                case command_code::lindex:
                    return _redis.lindex(_req, std::ref(out));
                case command_code::linsert:
                    return _redis.linsert(_req, std::ref(out));
                case command_code::lrange:
                    return _redis.lrange(_req, std::ref(out));
                case command_code::lset:
                    return _redis.lset(_req, std::ref(out));
                case command_code::rpush:
                    return _redis.rpush(_req, std::ref(out));
                case command_code::rpushx:
                    return _redis.rpushx(_req, std::ref(out));
                case command_code::rpop:
                    return _redis.rpop(_req, std::ref(out));
                case command_code::lrem:
                    return _redis.lrem(_req, std::ref(out));
                case command_code::ltrim:
                    return _redis.ltrim(_req, std::ref(out));
                case command_code::hset:
                    return _redis.hset(_req, std::ref(out));
                case command_code::hmset:
                    return _redis.hmset(_req, std::ref(out));
                case command_code::hdel:
                    return _redis.hdel(_req, std::ref(out));
                case command_code::hget:
                    return _redis.hget(_req, std::ref(out));
                case command_code::hlen:
                    return _redis.hlen(_req, std::ref(out));
                case command_code::hexists:
                    return _redis.hexists(_req, std::ref(out));
                case command_code::hstrlen:
                    return _redis.hstrlen(_req, std::ref(out));
                case command_code::hincrby:
                    return _redis.hincrby(_req, std::ref(out));
                case command_code::hincrbyfloat:
                    return _redis.hincrbyfloat(_req, std::ref(out));
                case command_code::hkeys:
                    return _redis.hgetall_keys(_req, std::ref(out));
                case command_code::hvals:
                    return _redis.hgetall_values(_req, std::ref(out));
                case command_code::hmget:
                    return _redis.hmget(_req, std::ref(out));
                case command_code::hgetall:
                    return _redis.hgetall(_req, std::ref(out));
                case command_code::sadd:
                    return _redis.sadd(_req, std::ref(out));
                case command_code::scard:
                    return _redis.scard(_req, std::ref(out));
                case command_code::sismember:
                    return _redis.sismember(_req, std::ref(out));
                case command_code::smembers:
                    return _redis.smembers(_req, std::ref(out));
                case command_code::srandmember:
                    return _redis.srandmember(_req, std::ref(out));
                case command_code::srem:
                    return _redis.srem(_req, std::ref(out));
                case command_code::sdiff:
                    return _redis.sdiff(_req,std::ref(out));
                case command_code::sdiffstore:
                    return _redis.sdiff_store(_req, std::ref(out));
                case command_code::sinter:
                    return _redis.sinter(_req, std::ref(out));
                case command_code::sinterstore:
                    return _redis.sinter_store(_req, std::ref(out));
                case command_code::sunion:
                    return _redis.sunion(_req, std::ref(out));
                case command_code::sunionstore:
                    return _redis.sunion_store(_req, std::ref(out));
                case command_code::smove:
                    return _redis.smove(_req, std::ref(out));
                case command_code::spop:
                    return _redis.spop(_req, std::ref(out));
                case command_code::type:
                    return _redis.type(_req, std::ref(out));
                case command_code::expire:
                    return _redis.expire(_req, std::ref(out));
                case command_code::pexpire:
                    return _redis.pexpire(_req, std::ref(out));
                case command_code::ttl:
                    return _redis.ttl(_req, std::ref(out));
                case command_code::pttl:
                    return _redis.pttl(_req, std::ref(out));
                case command_code::persist:
                    return _redis.persist(_req, std::ref(out));
                case command_code::zadd:
                    return _redis.zadd(_req, std::ref(out));
                case command_code::zrange:
                    return _redis.zrange(_req, false, std::ref(out));
                case command_code::zrevrange:
                    return _redis.zrange(_req, true, std::ref(out));
                case command_code::zrangebyscore:
                    return _redis.zrangebyscore(_req, false, std::ref(out));
                case command_code::zrevrangebyscore:
                    return _redis.zrangebyscore(_req, true, std::ref(out));
                case command_code::zrem:
                    return _redis.zrem(_req, std::ref(out));
                case command_code::zremrangebyscore:
                    return _redis.zremrangebyscore(_req, std::ref(out));
                case command_code::zremrangebyrank:
                    return _redis.zremrangebyrank(_req, std::ref(out));
                case command_code::zcard:
                    return _redis.zcard(_req, std::ref(out));
                case command_code::zcount:
                    return _redis.zcount(_req, std::ref(out));
                case command_code::zscore:
                    return _redis.zscore(_req, std::ref(out));
                case command_code::zincrby:
                    return _redis.zincrby(_req, std::ref(out));
                case command_code::zrank:
                    return _redis.zrank(_req, false, std::ref(out));
                case command_code::zrevrank:
                    return _redis.zrank(_req, true, std::ref(out));
                case command_code::zunionstore:
                    return _redis.zunionstore(_req, std::ref(out));
                case command_code::zinterstore:
                    return _redis.zinterstore(_req, std::ref(out));
                case command_code::select:
                    return _redis.select(_req, std::ref(out));
                case command_code::geoadd:
                    return _redis.geoadd(_req, std::ref(out));
                case command_code::geodist:
                    return _redis.geodist(_req, std::ref(out));
                case command_code::geopos:
                    return _redis.geopos(_req, std::ref(out));
                case command_code::geohash:
                    return _redis.geohash(_req, std::ref(out));
                case command_code::georadius:
                    return _redis.georadius(_req, false, std::ref(out));
                case command_code::georadiusbymember:
                    return _redis.georadius(_req, true, std::ref(out));
                case command_code::setbit:
                    return _redis.setbit(_req, std::ref(out));
                case command_code::getbit:
                    return _redis.getbit(_req, std::ref(out));
                case command_code::bitcount:
                    return _redis.bitcount(_req, std::ref(out));

                /*
                case command_code::bitpos:
                    return _redis.bitpos(_req).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case command_code::bitop:
                    return _redis.bitop(_req).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                */
                case command_code::pfadd:
                    return _redis.pfadd(_req, std::ref(out));
                case command_code::pfcount:
                    return _redis.pfcount(_req, std::ref(out));
                case command_code::pfmerge:
                    return _redis.pfmerge(_req, std::ref(out));
                default:
                    return out.write("+Not Implemented");
                };
            }
            default:
                return out.write("+Error\r\n");
        };
        std::abort();
    }).then_wrapped([this, &in, &out] (auto&& f) -> future<> {
        try {
            f.get();
        } catch (std::bad_alloc& e) {
            return out.write(msg_err);
        }
        return make_ready_future<>();
    });
}
}
