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

/** This protocol parser was inspired by the memcached app,
  which is the example in Seastar project.
**/

//#include "utils/redis_ragel.hh"
#include "redis_command_code.hh"
#include <memory>
#include <iostream>
#include <algorithm>
#include <functional>
#include "utils/bytes.hh"
#include "protocol_parser.hh"
using namespace seastar;
using namespace redis;
%%{

machine redis_resp_protocol;

access _fsm_;

action mark {
    g.mark_start(p);
}

action start_blob {
    g.mark_start(p);
    _size_left = _arg_size;
}
action start_command {
    g.mark_start(p);
    _size_left = _arg_size;
}

action advance_blob {
    auto len = std::min((uint32_t)(pe - p), _size_left);
    _size_left -= len;
    p += len;
    if (_size_left == 0) {
      _req._args.push_back(str());
      p--;
      fret;
    }
    p--;
}

action advance_command {
    auto len = std::min((uint32_t)(pe - p), _size_left);
    _size_left -= len;
    p += len;
    if (_size_left == 0) {
      _req._command = str();
      p--;
      fret;
    }
    p--;
}


crlf = '\r\n';
u32 = digit+ >{ _u32 = 0;}  ${ _u32 *= 10; _u32 += fc - '0';};
args_count = '*' u32 crlf ${_req._args_count = _u32 - 1;};
blob := any+ >start_blob $advance_blob;
set = "set"i ${_req._command = command_code::set;};
mset = "mset"i ${_req._command = command_code::mset;};
get = "get"i ${_req._command = command_code::get;};
mget = "mget"i ${_req._command = command_code::mget;};
del = "del"i ${_req._command = command_code::del;};
echo = "echo"i ${_req._command = command_code::echo;};
ping = "ping"i ${_req._command = command_code::ping;};
incr = "incr"i ${_req._command = command_code::incr;};
decr = "decr"i ${_req._command = command_code::decr;};
incrby = "incrby"i ${_req._command = command_code::incrby;};
decrby = "decrby"i ${_req._command = command_code::decrby;};
command_ = "command"i ${_req._command = command_code::command;};
exists = "exists"i ${_req._command = command_code::exists;};
append = "append"i ${_req._command = command_code::append;};
strlen = "strlen"i ${_req._command = command_code::strlen;};
lpush = "lpush"i ${_req._command = command_code::lpush;};
lpushx = "lpushx"i ${_req._command = command_code::lpushx;};
lpop = "lpop"i ${_req._command = command_code::lpop;};
llen = "llen"i ${_req._command = command_code::llen;};
lindex = "lindex"i ${_req._command = command_code::lindex;};
linsert = "linsert"i ${_req._command = command_code::linsert;};
lrange = "lrange"i ${_req._command = command_code::lrange;};
lset = "lset"i ${_req._command = command_code::lset;};
rpush = "rpush"i ${_req._command = command_code::rpush;};
rpushx = "rpushx"i ${_req._command = command_code::rpushx;};
rpop = "rpop"i ${_req._command = command_code::rpop;};
lrem = "lrem"i ${_req._command = command_code::lrem;};
ltrim = "ltrim"i ${_req._command = command_code::ltrim;};
hset = "hset"i ${_req._command = command_code::hset;};
hmset = "hmset"i ${_req._command = command_code::hmset;};
hdel = "hdel"i ${_req._command = command_code::hdel;};
hget = "hget"i ${_req._command = command_code::hget;};
hlen = "hlen"i ${_req._command = command_code::hlen;};
hexists = "hexists"i ${_req._command = command_code::hexists;};
hstrlen = "hstrlen"i ${_req._command = command_code::hstrlen;};
hincrby = "hincrby"i ${_req._command = command_code::hincrby;};
hincrbyfloat = "hincrbyfloat"i ${_req._command = command_code::hincrbyfloat;};
hkeys = "hkeys"i ${_req._command = command_code::hkeys;};
hvals = "hvals"i ${_req._command = command_code::hvals;};
hmget = "hmget"i ${_req._command = command_code::hmget;};
hgetall = "hgetall"i ${_req._command = command_code::hgetall;};
sadd = "sadd"i ${_req._command = command_code::sadd;};
scard = "scard"i ${_req._command = command_code::scard;};
sismember = "sismember"i ${_req._command = command_code::sismember;};
smembers = "smembers"i ${_req._command = command_code::smembers;};
srandmember = "srandmember"i ${_req._command = command_code::srandmember;};
srem = "srem"i ${_req._command = command_code::srem;};
sdiff = "sdiff"i ${_req._command = command_code::sdiff;};
sdiffstore = "sdiffstore"i ${_req._command = command_code::sdiffstore;};
sinter = "sinter"i ${_req._command = command_code::sinter;};
sinterstore = "sinterstore"i ${_req._command = command_code::sinterstore;};
sunion = "sunion"i ${_req._command = command_code::sunion;};
sunionstore = "sunionstore"i ${_req._command = command_code::sunionstore;};
smove = "smove"i ${_req._command = command_code::smove;};
spop = "spop"i ${_req._command = command_code::spop;};
type = "type"i ${_req._command = command_code::type; };
expire = "expire"i ${_req._command = command_code::expire; };
pexpire = "pexpire"i ${_req._command = command_code::pexpire; };
ttl = "ttl"i ${_req._command = command_code::ttl; };
pttl = "pttl"i ${_req._command = command_code::pttl; };
persist = "persist"i ${_req._command = command_code::persist; };
zadd = "zadd"i ${_req._command = command_code::zadd; };
zcard  = "zcard"i ${_req._command = command_code::zcard; };
zcount = "zcount"i ${_req._command = command_code::zcount; };
zincrby = "zincrby"i ${_req._command = command_code::zincrby; };
zrange = "zrange"i ${_req._command = command_code::zrange;};
zrank = "zrank"i ${_req._command = command_code::zrank; };
zrem = "zrem"i ${_req._command = command_code::zrem; };
zremrangebyrank = "zremrangebyrank"i ${_req._command = command_code::zremrangebyrank; };
zremrangebyscore = "zremrangebyscore"i ${_req._command = command_code::zremrangebyscore; };
zrevrange = "zrevrange"i ${_req._command = command_code::zrevrange; };
zrevrangebyscore = "zrevrangebyscore"i ${_req._command = command_code::zrevrangebyscore; };
zrevrank = "zrevrank"i ${_req._command = command_code::zrevrank; };
zscore = "zscore"i ${_req._command = command_code::zscore; };
zunionstore = "zunionstore"i ${_req._command = command_code::zunionstore; };
zinterstore = "zinterstore"i ${_req._command = command_code::zinterstore; };
zdiffstore = "zdiffstore"i ${_req._command = command_code::zdiffstore; };
zunion = "zunion"i ${_req._command = command_code::zunion; };
zinter = "zinter"i ${_req._command = command_code::zinter; };
zdiff = "zunion"i ${_req._command = command_code::zdiff; };
zscan = "zscan"i ${_req._command = command_code::zscan; };
zrangebylex = "zrangebylex"i ${_req._command = command_code::zrangebylex; };
zrangebyscore = "zrangebyscore"i ${_req._command = command_code::zrangebyscore; };
zlexcount = "zlexcount"i ${_req._command = command_code::zlexcount;};
zremrangebylex = "zremrangebylex"i ${_req._command = command_code::zremrangebylex; };
select = "select"i ${_req._command = command_code::select; };
geoadd = "geoadd"i ${_req._command = command_code::geoadd; };
geodist = "geodist"i ${_req._command = command_code::geodist; };
geohash = "geohash"i ${_req._command = command_code::geohash; };
geopos = "geopos"i ${_req._command = command_code::geopos; };
georadius = "georadius"i ${_req._command = command_code::georadius; };
georadiusbymember = "georadiusbymember"i ${_req._command = command_code::georadiusbymember; };
setbit = "setbit"i ${_req._command = command_code::setbit; };
getbit = "getbit"i ${_req._command = command_code::getbit; };
bitcount = "bitcount"i ${_req._command = command_code::bitcount; };
bitop = "bitop"i ${_req._command = command_code::bitop; };
bitfield = "bitfield"i ${_req._command = command_code::bitfield; };
bitpos = "bitpos"i ${_req._command = command_code::bitpos; };
pfadd = "pfadd"i ${_req._command = command_code::pfadd; };
pfcount = "pfcount"i ${_req._command = command_code::pfcount; };
pfmerge = "pfmerge"i ${_req._command = command_code::pfmerge; };

command = (setbit | set | getbit | get | del | mget | mset | echo | ping | incr | decr | incrby | decrby | command_ | exists | append |
           strlen | lpushx | lpush | lpop | llen | lindex | linsert | lrange | lset | rpushx | rpush | rpop | lrem |
           ltrim | hset | hgetall |hget | hdel | hlen | hexists | hstrlen | hincrby | hincrbyfloat | hkeys | hvals | hmget | hmset |
           sadd | scard | sismember | smembers | srem | sdiffstore | sdiff | sinterstore | sinter| sunionstore | sunion | smove | srandmember | spop |
           type | expire | pexpire | persist | ttl | pttl | zadd | zcard | zcount | zincrby |
           zrangebyscore | zrank | zremrangebyrank | zremrangebyscore | zremrangebylex | zrem | zrevrangebyscore | zrevrange| zrevrank |
           zscore | zunionstore  | zinterstore | zdiffstore | zunion | zinter | zdiff | zscan | zrangebylex | zlexcount |
           zrange | select | geoadd | geodist | geohash | geopos | georadiusbymember | georadius |  bitcount |
           bitpos | bitop | bitfield |
           pfadd | pfcount | pfmerge );
arg = '$' u32 crlf ${ _arg_size = _u32;};

main := (args_count (arg command crlf) (arg @{fcall blob; } crlf)+) ${_req._state = protocol_state::ok;};

prepush {
    prepush();
}

postpop {
    postpop();
}

}%%

namespace redis {

class bytes_builder {
    bytes _value;
    const char* _start = nullptr;
public:
    class guard;
public:
    bytes get() && {
        return std::move(_value);
    }
    void reset() {
        _value.reset();
        _start = nullptr;
    }
    friend class guard;
};

class bytes_builder::guard {
    bytes_builder& _builder;
    const char* _block_end;
public:
    guard(bytes_builder& builder, const char* block_start, const char* block_end)
        : _builder(builder), _block_end(block_end) {
        if (!_builder._value.empty()) {
            mark_start(block_start);
        }
    }
    ~guard() {
        if (_builder._start) {
            mark_end(_block_end);
        }
    }
    void mark_start(const char* p) {
        _builder._start = p;
    }
    void mark_end(const char* p) {
        if (_builder._value.empty()) {
            // avoid an allocation in the common case
            _builder._value = bytes(_builder._start, p);
        } else {
            _builder._value += bytes(_builder._start, p);
        }
        _builder._start = nullptr;
    }
};

class ragel_protocol_parser : public protocol_parser::impl {
    %% write data nofinal noprefix;
protected:
    int _fsm_cs;
    std::unique_ptr<int[]> _fsm_stack = nullptr;
    int _fsm_stack_size = 0;
    int _fsm_top;
    int _fsm_act;
    char* _fsm_ts;
    char* _fsm_te;
    bytes_builder _builder;

    void init_base() {
        _builder.reset();
    }
    void prepush() {
        if (_fsm_top == _fsm_stack_size) {
            auto old = _fsm_stack_size;
            _fsm_stack_size = std::max(_fsm_stack_size * 2, 16);
            assert(_fsm_stack_size > old);
            std::unique_ptr<int[]> new_stack{new int[_fsm_stack_size]};
            std::copy(_fsm_stack.get(), _fsm_stack.get() + _fsm_top, new_stack.get());
            std::swap(_fsm_stack, new_stack);
        }
    }
    void postpop() {}
    bytes get_str() {
        auto s = std::move(_builder).get();
        return std::move(s);
    }
public:
    request_wrapper _req;
    uint32_t _u32;
    uint32_t _arg_size;
    uint32_t _size_left;
public:
    ragel_protocol_parser() {}
    virtual ~ragel_protocol_parser() {}

    virtual void init() {
        init_base();
        _req._state = protocol_state::error;
        _req._args.clear();
        _req._args_count = 0;
        _size_left = 0;
        _arg_size = 0;
        %% write init;
    }

    virtual char* parse(char* p, char* pe, char* eof) {
        bytes_builder::guard g(_builder, p, pe);
        auto str = [this, &g, &p] {
            g.mark_end(p);
            auto s =  get_str();
            return s;
        };

        %% write exec;
        if (_req._state != protocol_state::error) {
            return p;
        }
        // error ?
        if (p != pe) {
            p = pe;
            return p;
        }
        return nullptr;
    }
    bool eof() const {
        return _req._state == protocol_state::eof;
    }
    virtual request_wrapper& request() { return _req; }
};

}

