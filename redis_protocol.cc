/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
#include "base.hh"
#include <algorithm>
namespace redis {

redis_protocol::redis_protocol(redis_service& redis) : _redis(redis)
{
}

void redis_protocol::prepare_request()
{
    _command_args._command_args_count = _parser._args_count - 1;
    _command_args._command_args = std::move(_parser._args_list);
}

future<> redis_protocol::handle(input_stream<char>& in, output_stream<char>& out)
{
    _parser.init();
    return in.consume(_parser).then([this, &in, &out] () -> future<> {
        switch (_parser._state) {
            case redis_protocol_parser::state::eof:
            case redis_protocol_parser::state::error:
                return make_ready_future<>();

            case redis_protocol_parser::state::ok:
            {
                prepare_request();
                switch (_parser._command) {
                case redis_protocol_parser::command::set:
                    return _redis.set(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::mset:
                    return _redis.mset(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::get:
                    return _redis.get(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::del:
                    return _redis.del(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::ping:
                    return out.write(msg_pong);
                case redis_protocol_parser::command::incr:
                    return _redis.incr(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::decr:
                    return _redis.incr(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::incrby:
                    return _redis.incrby(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::decrby:
                    return _redis.decrby(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::mget:
                    return _redis.mget(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::command:
                    return out.write(msg_ok);
                case redis_protocol_parser::command::exists:
                    return _redis.exists(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::append:
                    return _redis.append(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::strlen:
                    return _redis.strlen(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::lpush:
                    return _redis.lpush(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::lpushx:
                    return _redis.lpushx(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::lpop:
                    return _redis.lpop(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::llen:
                    return _redis.llen(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::lindex:
                    return _redis.lindex(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::linsert:
                    return _redis.linsert(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::lrange:
                    return _redis.lrange(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::lset:
                    return _redis.lset(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::rpush:
                    return _redis.rpush(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::rpushx:
                    return _redis.rpushx(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::rpop:
                    return _redis.rpop(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::lrem:
                    return _redis.lrem(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::ltrim:
                    return _redis.ltrim(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::hset:
                    return _redis.hset(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::hdel:
                    return _redis.hdel(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::hget:
                    return _redis.hget(_command_args).then([ &out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::hlen:
                    return _redis.hlen(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::hexists:
                    return _redis.hexists(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::hstrlen:
                    return _redis.hstrlen(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::hincrby:
                    return _redis.hincrby(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::hincrbyfloat:
                    return _redis.hincrbyfloat(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::hkeys:
                    return _redis.hgetall(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::hvals:
                    return _redis.hgetall(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::hmget:
                    return _redis.hmget(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::hgetall:
                    return _redis.hgetall(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::sadd:
                    return _redis.sadd(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::scard:
                    return _redis.scard(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::sismember:
                    return _redis.sismember(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::smembers:
                    return _redis.smembers(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::srem:
                    return _redis.srem(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::sdiff:
                    return _redis.sdiff(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::sdiffstore:
                    return _redis.sdiff_store(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::sinter:
                    return _redis.sinter(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::sinterstore:
                    return _redis.sinter_store(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::sunion:
                    return _redis.sunion(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::sunionstore:
                    return _redis.sunion_store(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::smove:
                    return _redis.smove(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::type:
                    return _redis.type(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::expire:
                    return _redis.expire(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::pexpire:
                    return _redis.pexpire(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::ttl:
                    return _redis.ttl(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::pttl:
                    return _redis.pttl(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::persist:
                    return _redis.persist(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::zadd:
                    return _redis.zadd(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::zrange:
                    return _redis.zrange(_command_args, false).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::zrevrange:
                    return _redis.zrange(_command_args, true).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::zrangebyscore:
                    return _redis.zrangebyscore(_command_args, false).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::zrevrangebyscore:
                    return _redis.zrangebyscore(_command_args, true).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::zrem:
                    return _redis.zrem(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::zcard:
                    return _redis.zcard(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::zcount:
                    return _redis.zcount(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::zscore:
                    return _redis.zscore(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::zincrby:
                    return _redis.zincrby(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::zrank:
                    return _redis.zrank(_command_args, false).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::zrevrank:
                    return _redis.zrank(_command_args, true).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
                case redis_protocol_parser::command::zunionstore:
                    return _redis.zunionstore(_command_args).then([&out] (auto&& m) {
                        return out.write(std::move(m));
                    });
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
            in.close();
            return out.write(msg_err);
        }
        return make_ready_future<>();
    });
}
}
