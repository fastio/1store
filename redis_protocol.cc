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

redis_protocol::redis_protocol()
{
}

void redis_protocol::prepare_request()
{
    _request_args._command_args_count = _parser._args_count - 1;
    _request_args._command_args = std::move(_parser._args_list);
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
                    return local_redis_service().set(_request_args, std::ref(out));
                case redis_protocol_parser::command::mset:
                    return local_redis_service().mset(_request_args, std::ref(out));
                case redis_protocol_parser::command::get:
                    return local_redis_service().get(_request_args, std::ref(out));
                case redis_protocol_parser::command::del:
                    return local_redis_service().del(_request_args, std::ref(out));
                case redis_protocol_parser::command::ping:
                    return out.write(msg_pong);
                case redis_protocol_parser::command::incr:
                    return local_redis_service().incr(_request_args, std::ref(out));
                case redis_protocol_parser::command::decr:
                    return local_redis_service().incr(_request_args, std::ref(out));
                case redis_protocol_parser::command::incrby:
                    return local_redis_service().incrby(_request_args, std::ref(out));
                case redis_protocol_parser::command::decrby:
                    return local_redis_service().decrby(_request_args, std::ref(out));
                case redis_protocol_parser::command::mget:
                    return local_redis_service().mget(_request_args, out);
                case redis_protocol_parser::command::command:
                    return out.write(msg_ok);
                case redis_protocol_parser::command::exists:
                    return local_redis_service().exists(_request_args, std::ref(out));
                case redis_protocol_parser::command::append:
                    return local_redis_service().append(_request_args, std::ref(out));
                case redis_protocol_parser::command::strlen:
                    return local_redis_service().strlen(_request_args, std::ref(out));
                case redis_protocol_parser::command::lpush:
                    return local_redis_service().lpush(_request_args, std::ref(out));
                case redis_protocol_parser::command::lpushx:
                    return local_redis_service().lpushx(_request_args, std::ref(out));
                case redis_protocol_parser::command::lpop:
                    return local_redis_service().lpop(_request_args, std::ref(out));
                case redis_protocol_parser::command::llen:
                    return local_redis_service().llen(_request_args, std::ref(out));
                case redis_protocol_parser::command::lindex:
                    return local_redis_service().lindex(_request_args, std::ref(out));
                case redis_protocol_parser::command::linsert:
                    return local_redis_service().linsert(_request_args, std::ref(out));
                case redis_protocol_parser::command::lrange:
                    return local_redis_service().lrange(_request_args, std::ref(out));
                case redis_protocol_parser::command::lset:
                    return local_redis_service().lset(_request_args, std::ref(out));
                case redis_protocol_parser::command::rpush:
                    return local_redis_service().rpush(_request_args, std::ref(out));
                case redis_protocol_parser::command::rpushx:
                    return local_redis_service().rpushx(_request_args, std::ref(out));
                case redis_protocol_parser::command::rpop:
                    return local_redis_service().rpop(_request_args, std::ref(out));
                case redis_protocol_parser::command::lrem:
                    return local_redis_service().lrem(_request_args, std::ref(out));
                case redis_protocol_parser::command::ltrim:
                    return local_redis_service().ltrim(_request_args, std::ref(out));
                case redis_protocol_parser::command::hset:
                    return local_redis_service().hset(_request_args, std::ref(out));
                case redis_protocol_parser::command::hmset:
                    return local_redis_service().hmset(_request_args, std::ref(out));
                case redis_protocol_parser::command::hdel:
                    return local_redis_service().hdel(_request_args, std::ref(out));
                case redis_protocol_parser::command::hget:
                    return local_redis_service().hget(_request_args, std::ref(out));
                case redis_protocol_parser::command::hlen:
                    return local_redis_service().hlen(_request_args, std::ref(out));
                case redis_protocol_parser::command::hexists:
                    return local_redis_service().hexists(_request_args, std::ref(out));
                case redis_protocol_parser::command::hstrlen:
                    return local_redis_service().hstrlen(_request_args, std::ref(out));
                case redis_protocol_parser::command::hincrby:
                    return local_redis_service().hincrby(_request_args, std::ref(out));
                case redis_protocol_parser::command::hincrbyfloat:
                    return local_redis_service().hincrbyfloat(_request_args, std::ref(out));
                case redis_protocol_parser::command::hkeys:
                    return local_redis_service().hgetall_keys(_request_args, std::ref(out));
                case redis_protocol_parser::command::hvals:
                    return local_redis_service().hgetall_values(_request_args, std::ref(out));
                case redis_protocol_parser::command::hmget:
                    return local_redis_service().hmget(_request_args, std::ref(out));
                case redis_protocol_parser::command::hgetall:
                    return local_redis_service().hgetall(_request_args, std::ref(out));
                case redis_protocol_parser::command::sadd:
                    return local_redis_service().sadd(_request_args, std::ref(out));
                case redis_protocol_parser::command::scard:
                    return local_redis_service().scard(_request_args, std::ref(out));
                case redis_protocol_parser::command::sismember:
                    return local_redis_service().sismember(_request_args, std::ref(out));
                case redis_protocol_parser::command::smembers:
                    return local_redis_service().smembers(_request_args, std::ref(out));
                case redis_protocol_parser::command::srandmember:
                    return local_redis_service().srandmember(_request_args, std::ref(out));
                case redis_protocol_parser::command::srem:
                    return local_redis_service().srem(_request_args, std::ref(out));
                case redis_protocol_parser::command::sdiff:
                    return local_redis_service().sdiff(_request_args,std::ref(out));
                case redis_protocol_parser::command::sdiffstore:
                    return local_redis_service().sdiff_store(_request_args, std::ref(out));
                case redis_protocol_parser::command::sinter:
                    return local_redis_service().sinter(_request_args, std::ref(out));
                case redis_protocol_parser::command::sinterstore:
                    return local_redis_service().sinter_store(_request_args, std::ref(out));
                case redis_protocol_parser::command::sunion:
                    return local_redis_service().sunion(_request_args, std::ref(out));
                case redis_protocol_parser::command::sunionstore:
                    return local_redis_service().sunion_store(_request_args, std::ref(out));
                case redis_protocol_parser::command::smove:
                    return local_redis_service().smove(_request_args, std::ref(out));
                case redis_protocol_parser::command::spop:
                    return local_redis_service().spop(_request_args, std::ref(out));
                case redis_protocol_parser::command::type:
                    return local_redis_service().type(_request_args, std::ref(out));
                case redis_protocol_parser::command::expire:
                    return local_redis_service().expire(_request_args, std::ref(out));
                case redis_protocol_parser::command::pexpire:
                    return local_redis_service().pexpire(_request_args, std::ref(out));
                case redis_protocol_parser::command::ttl:
                    return local_redis_service().ttl(_request_args, std::ref(out));
                case redis_protocol_parser::command::pttl:
                    return local_redis_service().pttl(_request_args, std::ref(out));
                case redis_protocol_parser::command::persist:
                    return local_redis_service().persist(_request_args, std::ref(out));
                case redis_protocol_parser::command::zadd:
                    return local_redis_service().zadd(_request_args, std::ref(out));
                case redis_protocol_parser::command::zrange:
                    return local_redis_service().zrange(_request_args, false, std::ref(out));
                case redis_protocol_parser::command::zrevrange:
                    return local_redis_service().zrange(_request_args, true, std::ref(out));
                case redis_protocol_parser::command::zrangebyscore:
                    return local_redis_service().zrangebyscore(_request_args, false, std::ref(out));
                case redis_protocol_parser::command::zrevrangebyscore:
                    return local_redis_service().zrangebyscore(_request_args, true, std::ref(out));
                case redis_protocol_parser::command::zrem:
                    return local_redis_service().zrem(_request_args, std::ref(out));
                case redis_protocol_parser::command::zremrangebyscore:
                    return local_redis_service().zremrangebyscore(_request_args, std::ref(out));
                case redis_protocol_parser::command::zremrangebyrank:
                    return local_redis_service().zremrangebyrank(_request_args, std::ref(out));
                case redis_protocol_parser::command::zcard:
                    return local_redis_service().zcard(_request_args, std::ref(out));
                case redis_protocol_parser::command::zcount:
                    return local_redis_service().zcount(_request_args, std::ref(out));
                case redis_protocol_parser::command::zscore:
                    return local_redis_service().zscore(_request_args, std::ref(out));
                case redis_protocol_parser::command::zincrby:
                    return local_redis_service().zincrby(_request_args, std::ref(out));
                case redis_protocol_parser::command::zrank:
                    return local_redis_service().zrank(_request_args, false, std::ref(out));
                case redis_protocol_parser::command::zrevrank:
                    return local_redis_service().zrank(_request_args, true, std::ref(out));
                case redis_protocol_parser::command::zunionstore:
                    return local_redis_service().zunionstore(_request_args, std::ref(out));
                case redis_protocol_parser::command::zinterstore:
                    return local_redis_service().zinterstore(_request_args, std::ref(out));
                case redis_protocol_parser::command::select:
                    return local_redis_service().select(_request_args, std::ref(out));
                case redis_protocol_parser::command::geoadd:
                    return local_redis_service().geoadd(_request_args, std::ref(out));
                case redis_protocol_parser::command::geodist:
                    return local_redis_service().geodist(_request_args, std::ref(out));
                case redis_protocol_parser::command::geopos:
                    return local_redis_service().geopos(_request_args, std::ref(out));
                case redis_protocol_parser::command::geohash:
                    return local_redis_service().geohash(_request_args, std::ref(out));
                case redis_protocol_parser::command::georadius:
                    return local_redis_service().georadius(_request_args, false, std::ref(out));
                case redis_protocol_parser::command::georadiusbymember:
                    return local_redis_service().georadius(_request_args, true, std::ref(out));
                case redis_protocol_parser::command::setbit:
                    return local_redis_service().setbit(_request_args, std::ref(out));
                case redis_protocol_parser::command::getbit:
                    return local_redis_service().getbit(_request_args, std::ref(out));
                case redis_protocol_parser::command::bitcount:
                    return local_redis_service().bitcount(_request_args, std::ref(out));

                /*
                case redis_protocol_parser::command::bitpos:
                    return local_redis_service().bitpos(_request_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::bitop:
                    return local_redis_service().bitop(_request_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                */
                case redis_protocol_parser::command::pfadd:
                    return local_redis_service().pfadd(_request_args, std::ref(out));
                case redis_protocol_parser::command::pfcount:
                    return local_redis_service().pfcount(_request_args, std::ref(out));
                case redis_protocol_parser::command::pfmerge:
                    return local_redis_service().pfmerge(_request_args, std::ref(out));
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
