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
#include "common.hh"
#include <algorithm>

namespace redis {

redis_protocol::redis_protocol(redis_service& redis) : _redis(redis)
{
}

void redis_protocol::prepare_request()
{
    _request_args._request_args_count = _parser._args_count - 1;
    _request_args._request_args = std::move(_parser._args_list);
    _request_args._tmp_keys.clear();
    _request_args._tmp_key_values.clear();
    _request_args._tmp_key_scores.clear();
    _request_args._tmp_key_value_pairs.clear();
}

future<> redis_protocol::handle(input_stream<char>& in, output_stream<char>& out, request_latency_tracer& tracer)
{
    _parser.init();
    // NOTE: The command is handled sequentially. The parser will control the lifetime
    // of every parameters for command.
    return in.consume(_parser).then([this, &in, &out, &tracer] () -> future<> {
        tracer.begin_trace_latency();
        switch (_parser._state) {
            case redis_protocol_parser::state::eof:
            case redis_protocol_parser::state::error:
                return make_ready_future<>();

            case redis_protocol_parser::state::ok:
            {
                prepare_request();
                switch (_parser._command) {
                case redis_protocol_parser::command::set:
                    return _redis.set(_request_args, std::ref(out));
                case redis_protocol_parser::command::mset:
                    return _redis.mset(_request_args, std::ref(out));
                case redis_protocol_parser::command::get:
                    return _redis.get(_request_args, std::ref(out));
                case redis_protocol_parser::command::del:
                    return _redis.del(_request_args, std::ref(out));
                case redis_protocol_parser::command::ping:
                    return out.write(msg_pong);
                case redis_protocol_parser::command::incr:
                    return _redis.incr(_request_args, std::ref(out));
                case redis_protocol_parser::command::decr:
                    return _redis.incr(_request_args, std::ref(out));
                case redis_protocol_parser::command::incrby:
                    return _redis.incrby(_request_args, std::ref(out));
                case redis_protocol_parser::command::decrby:
                    return _redis.decrby(_request_args, std::ref(out));
                case redis_protocol_parser::command::mget:
                    return _redis.mget(_request_args, out);
                case redis_protocol_parser::command::command:
                    return out.write(msg_ok);
                case redis_protocol_parser::command::exists:
                    return _redis.exists(_request_args, std::ref(out));
                case redis_protocol_parser::command::append:
                    return _redis.append(_request_args, std::ref(out));
                case redis_protocol_parser::command::strlen:
                    return _redis.strlen(_request_args, std::ref(out));
                case redis_protocol_parser::command::lpush:
                    return _redis.lpush(_request_args, std::ref(out));
                case redis_protocol_parser::command::lpushx:
                    return _redis.lpushx(_request_args, std::ref(out));
                case redis_protocol_parser::command::lpop:
                    return _redis.lpop(_request_args, std::ref(out));
                case redis_protocol_parser::command::llen:
                    return _redis.llen(_request_args, std::ref(out));
                case redis_protocol_parser::command::lindex:
                    return _redis.lindex(_request_args, std::ref(out));
                case redis_protocol_parser::command::linsert:
                    return _redis.linsert(_request_args, std::ref(out));
                case redis_protocol_parser::command::lrange:
                    return _redis.lrange(_request_args, std::ref(out));
                case redis_protocol_parser::command::lset:
                    return _redis.lset(_request_args, std::ref(out));
                case redis_protocol_parser::command::rpush:
                    return _redis.rpush(_request_args, std::ref(out));
                case redis_protocol_parser::command::rpushx:
                    return _redis.rpushx(_request_args, std::ref(out));
                case redis_protocol_parser::command::rpop:
                    return _redis.rpop(_request_args, std::ref(out));
                case redis_protocol_parser::command::lrem:
                    return _redis.lrem(_request_args, std::ref(out));
                case redis_protocol_parser::command::ltrim:
                    return _redis.ltrim(_request_args, std::ref(out));
                case redis_protocol_parser::command::hset:
                    return _redis.hset(_request_args, std::ref(out));
                case redis_protocol_parser::command::hmset:
                    return _redis.hmset(_request_args, std::ref(out));
                case redis_protocol_parser::command::hdel:
                    return _redis.hdel(_request_args, std::ref(out));
                case redis_protocol_parser::command::hget:
                    return _redis.hget(_request_args, std::ref(out));
                case redis_protocol_parser::command::hlen:
                    return _redis.hlen(_request_args, std::ref(out));
                case redis_protocol_parser::command::hexists:
                    return _redis.hexists(_request_args, std::ref(out));
                case redis_protocol_parser::command::hstrlen:
                    return _redis.hstrlen(_request_args, std::ref(out));
                case redis_protocol_parser::command::hincrby:
                    return _redis.hincrby(_request_args, std::ref(out));
                case redis_protocol_parser::command::hincrbyfloat:
                    return _redis.hincrbyfloat(_request_args, std::ref(out));
                case redis_protocol_parser::command::hkeys:
                    return _redis.hgetall_keys(_request_args, std::ref(out));
                case redis_protocol_parser::command::hvals:
                    return _redis.hgetall_values(_request_args, std::ref(out));
                case redis_protocol_parser::command::hmget:
                    return _redis.hmget(_request_args, std::ref(out));
                case redis_protocol_parser::command::hgetall:
                    return _redis.hgetall(_request_args, std::ref(out));
                case redis_protocol_parser::command::sadd:
                    return _redis.sadd(_request_args, std::ref(out));
                case redis_protocol_parser::command::scard:
                    return _redis.scard(_request_args, std::ref(out));
                case redis_protocol_parser::command::sismember:
                    return _redis.sismember(_request_args, std::ref(out));
                case redis_protocol_parser::command::smembers:
                    return _redis.smembers(_request_args, std::ref(out));
                case redis_protocol_parser::command::srandmember:
                    return _redis.srandmember(_request_args, std::ref(out));
                case redis_protocol_parser::command::srem:
                    return _redis.srem(_request_args, std::ref(out));
                case redis_protocol_parser::command::sdiff:
                    return _redis.sdiff(_request_args,std::ref(out));
                case redis_protocol_parser::command::sdiffstore:
                    return _redis.sdiff_store(_request_args, std::ref(out));
                case redis_protocol_parser::command::sinter:
                    return _redis.sinter(_request_args, std::ref(out));
                case redis_protocol_parser::command::sinterstore:
                    return _redis.sinter_store(_request_args, std::ref(out));
                case redis_protocol_parser::command::sunion:
                    return _redis.sunion(_request_args, std::ref(out));
                case redis_protocol_parser::command::sunionstore:
                    return _redis.sunion_store(_request_args, std::ref(out));
                case redis_protocol_parser::command::smove:
                    return _redis.smove(_request_args, std::ref(out));
                case redis_protocol_parser::command::spop:
                    return _redis.spop(_request_args, std::ref(out));
                case redis_protocol_parser::command::type:
                    return _redis.type(_request_args, std::ref(out));
                case redis_protocol_parser::command::expire:
                    return _redis.expire(_request_args, std::ref(out));
                case redis_protocol_parser::command::pexpire:
                    return _redis.pexpire(_request_args, std::ref(out));
                case redis_protocol_parser::command::ttl:
                    return _redis.ttl(_request_args, std::ref(out));
                case redis_protocol_parser::command::pttl:
                    return _redis.pttl(_request_args, std::ref(out));
                case redis_protocol_parser::command::persist:
                    return _redis.persist(_request_args, std::ref(out));
                case redis_protocol_parser::command::zadd:
                    return _redis.zadd(_request_args, std::ref(out));
                case redis_protocol_parser::command::zrange:
                    return _redis.zrange(_request_args, false, std::ref(out));
                case redis_protocol_parser::command::zrevrange:
                    return _redis.zrange(_request_args, true, std::ref(out));
                case redis_protocol_parser::command::zrangebyscore:
                    return _redis.zrangebyscore(_request_args, false, std::ref(out));
                case redis_protocol_parser::command::zrevrangebyscore:
                    return _redis.zrangebyscore(_request_args, true, std::ref(out));
                case redis_protocol_parser::command::zrem:
                    return _redis.zrem(_request_args, std::ref(out));
                case redis_protocol_parser::command::zremrangebyscore:
                    return _redis.zremrangebyscore(_request_args, std::ref(out));
                case redis_protocol_parser::command::zremrangebyrank:
                    return _redis.zremrangebyrank(_request_args, std::ref(out));
                case redis_protocol_parser::command::zcard:
                    return _redis.zcard(_request_args, std::ref(out));
                case redis_protocol_parser::command::zcount:
                    return _redis.zcount(_request_args, std::ref(out));
                case redis_protocol_parser::command::zscore:
                    return _redis.zscore(_request_args, std::ref(out));
                case redis_protocol_parser::command::zincrby:
                    return _redis.zincrby(_request_args, std::ref(out));
                case redis_protocol_parser::command::zrank:
                    return _redis.zrank(_request_args, false, std::ref(out));
                case redis_protocol_parser::command::zrevrank:
                    return _redis.zrank(_request_args, true, std::ref(out));
                case redis_protocol_parser::command::zunionstore:
                    return _redis.zunionstore(_request_args, std::ref(out));
                case redis_protocol_parser::command::zinterstore:
                    return _redis.zinterstore(_request_args, std::ref(out));
                case redis_protocol_parser::command::select:
                    return _redis.select(_request_args, std::ref(out));
                case redis_protocol_parser::command::geoadd:
                    return _redis.geoadd(_request_args, std::ref(out));
                case redis_protocol_parser::command::geodist:
                    return _redis.geodist(_request_args, std::ref(out));
                case redis_protocol_parser::command::geopos:
                    return _redis.geopos(_request_args, std::ref(out));
                case redis_protocol_parser::command::geohash:
                    return _redis.geohash(_request_args, std::ref(out));
                case redis_protocol_parser::command::georadius:
                    return _redis.georadius(_request_args, false, std::ref(out));
                case redis_protocol_parser::command::georadiusbymember:
                    return _redis.georadius(_request_args, true, std::ref(out));
                case redis_protocol_parser::command::setbit:
                    return _redis.setbit(_request_args, std::ref(out));
                case redis_protocol_parser::command::getbit:
                    return _redis.getbit(_request_args, std::ref(out));
                case redis_protocol_parser::command::bitcount:
                    return _redis.bitcount(_request_args, std::ref(out));

                /*
                case redis_protocol_parser::command::bitpos:
                    return _redis.bitpos(_request_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::bitop:
                    return _redis.bitop(_request_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                */
                case redis_protocol_parser::command::pfadd:
                    return _redis.pfadd(_request_args, std::ref(out));
                case redis_protocol_parser::command::pfcount:
                    return _redis.pfcount(_request_args, std::ref(out));
                case redis_protocol_parser::command::pfmerge:
                    return _redis.pfmerge(_request_args, std::ref(out));
                default:
                    tracer.incr_number_exceptions();
                    return out.write("+Not Implemented");
                };
            }
            default:
                tracer.incr_number_exceptions();
                return out.write("+Error\r\n");
        };
        std::abort();
    }).then_wrapped([this, &in, &out, &tracer] (auto&& f) -> future<> {
        try {
            f.get();
        } catch (std::bad_alloc& e) {
            tracer.incr_number_exceptions();
            tracer.end_trace_latency();
            return out.write(msg_err);
        }
        tracer.end_trace_latency();
        return make_ready_future<>();
    });
}
}
