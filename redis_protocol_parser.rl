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

#include "ragel.hh"
#include "redis_command_code.hh"
#include <memory>
#include <iostream>
#include <algorithm>
#include <functional>
#include "bytes.hh"
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
      _args_list.push_back(str());
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
      _command = str();
      p--;
      fret;
    }
    p--;
}


crlf = '\r\n';
u32 = digit+ >{ _u32 = 0;}  ${ _u32 *= 10; _u32 += fc - '0';};
args_count = '*' u32 crlf ${_args_count = _u32;};
blob := any+ >start_blob $advance_blob;
set = "set"i ${_command = command_code::set;};
mset = "mset"i ${_command = command_code::mset;};
get = "get"i ${_command = command_code::get;};
mget = "mget"i ${_command = command_code::mget;};
del = "del"i ${_command = command_code::del;};
echo = "echo"i ${_command = command_code::echo;};
ping = "ping"i ${_command = command_code::ping;};
incr = "incr"i ${_command = command_code::incr;};
decr = "decr"i ${_command = command_code::decr;};
incrby = "incrby"i ${_command = command_code::incrby;};
decrby = "decrby"i ${_command = command_code::decrby;};
command_ = "command"i ${_command = command_code::command;};
exists = "exists"i ${_command = command_code::exists;};
append = "append"i ${_command = command_code::append;};
strlen = "strlen"i ${_command = command_code::strlen;};
lpush = "lpush"i ${_command = command_code::lpush;};
lpushx = "lpushx"i ${_command = command_code::lpushx;};
lpop = "lpop"i ${_command = command_code::lpop;};
llen = "llen"i ${_command = command_code::llen;};
lindex = "lindex"i ${_command = command_code::lindex;};
linsert = "linsert"i ${_command = command_code::linsert;};
lrange = "lrange"i ${_command = command_code::lrange;};
lset = "lset"i ${_command = command_code::lset;};
rpush = "rpush"i ${_command = command_code::rpush;};
rpushx = "rpushx"i ${_command = command_code::rpushx;};
rpop = "rpop"i ${_command = command_code::rpop;};
lrem = "lrem"i ${_command = command_code::lrem;};
ltrim = "ltrim"i ${_command = command_code::ltrim;};
hset = "hset"i ${_command = command_code::hset;};
hmset = "hmset"i ${_command = command_code::hmset;};
hdel = "hdel"i ${_command = command_code::hdel;};
hget = "hget"i ${_command = command_code::hget;};
hlen = "hlen"i ${_command = command_code::hlen;};
hexists = "hexists"i ${_command = command_code::hexists;};
hstrlen = "hstrlen"i ${_command = command_code::hstrlen;};
hincrby = "hincrby"i ${_command = command_code::hincrby;};
hincrbyfloat = "hincrbyfloat"i ${_command = command_code::hincrbyfloat;};
hkeys = "hkeys"i ${_command = command_code::hkeys;};
hvals = "hvals"i ${_command = command_code::hvals;};
hmget = "hmget"i ${_command = command_code::hmget;};
hgetall = "hgetall"i ${_command = command_code::hgetall;};
sadd = "sadd"i ${_command = command_code::sadd;};
scard = "scard"i ${_command = command_code::scard;};
sismember = "sismember"i ${_command = command_code::sismember;};
smembers = "smembers"i ${_command = command_code::smembers;};
srandmember = "srandmember"i ${_command = command_code::srandmember;};
srem = "srem"i ${_command = command_code::srem;};
sdiff = "sdiff"i ${_command = command_code::sdiff;};
sdiffstore = "sdiffstore"i ${_command = command_code::sdiffstore;};
sinter = "sinter"i ${_command = command_code::sinter;};
sinterstore = "sinterstore"i ${_command = command_code::sinterstore;};
sunion = "sunion"i ${_command = command_code::sunion;};
sunionstore = "sunionstore"i ${_command = command_code::sunionstore;};
smove = "smove"i ${_command = command_code::smove;};
spop = "spop"i ${_command = command_code::spop;};
type = "type"i ${_command = command_code::type; };
expire = "expire"i ${_command = command_code::expire; };
pexpire = "pexpire"i ${_command = command_code::pexpire; };
ttl = "ttl"i ${_command = command_code::ttl; };
pttl = "pttl"i ${_command = command_code::pttl; };
persist = "persist"i ${_command = command_code::persist; };
zadd = "zadd"i ${_command = command_code::zadd; };
zcard  = "zcard"i ${_command = command_code::zcard; };
zcount = "zcount"i ${_command = command_code::zcount; };
zincrby = "zincrby"i ${_command = command_code::zincrby; };
zrange = "zrange"i ${_command = command_code::zrange;};
zrank = "zrank"i ${_command = command_code::zrank; };
zrem = "zrem"i ${_command = command_code::zrem; };
zremrangebyrank = "zremrangebyrank"i ${_command = command_code::zremrangebyrank; };
zremrangebyscore = "zremrangebyscore"i ${_command = command_code::zremrangebyscore; };
zrevrange = "zrevrange"i ${_command = command_code::zrevrange; };
zrevrangebyscore = "zrevrangebyscore"i ${_command = command_code::zrevrangebyscore; };
zrevrank = "zrevrank"i ${_command = command_code::zrevrank; };
zscore = "zscore"i ${_command = command_code::zscore; };
zunionstore = "zunionstore"i ${_command = command_code::zunionstore; };
zinterstore = "zinterstore"i ${_command = command_code::zinterstore; };
zdiffstore = "zdiffstore"i ${_command = command_code::zdiffstore; };
zunion = "zunion"i ${_command = command_code::zunion; };
zinter = "zinter"i ${_command = command_code::zinter; };
zdiff = "zunion"i ${_command = command_code::zdiff; };
zscan = "zscan"i ${_command = command_code::zscan; };
zrangebylex = "zrangebylex"i ${_command = command_code::zrangebylex; };
zrangebyscore = "zrangebyscore"i ${_command = command_code::zrangebyscore; };
zlexcount = "zlexcount"i ${_command = command_code::zlexcount;};
zremrangebylex = "zremrangebylex"i ${_command = command_code::zremrangebylex; };
select = "select"i ${_command = command_code::select; };
geoadd = "geoadd"i ${_command = command_code::geoadd; };
geodist = "geodist"i ${_command = command_code::geodist; };
geohash = "geohash"i ${_command = command_code::geohash; };
geopos = "geopos"i ${_command = command_code::geopos; };
georadius = "georadius"i ${_command = command_code::georadius; };
georadiusbymember = "georadiusbymember"i ${_command = command_code::georadiusbymember; };
setbit = "setbit"i ${_command = command_code::setbit; };
getbit = "getbit"i ${_command = command_code::getbit; };
bitcount = "bitcount"i ${_command = command_code::bitcount; };
bitop = "bitop"i ${_command = command_code::bitop; };
bitfield = "bitfield"i ${_command = command_code::bitfield; };
bitpos = "bitpos"i ${_command = command_code::bitpos; };
pfadd = "pfadd"i ${_command = command_code::pfadd; };
pfcount = "pfcount"i ${_command = command_code::pfcount; };
pfmerge = "pfmerge"i ${_command = command_code::pfmerge; };

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

main := (args_count (arg command crlf) (arg @{fcall blob; } crlf)+) ${_state = protocol_state::ok;};

prepush {
    prepush();
}

postpop {
    postpop();
}

}%%

class redis_protocol_parser : public ragel_parser_base<redis_protocol_parser> {
    %% write data nofinal noprefix;
public:

    protocol_state _state;
    command_code _command;
    uint32_t _u32;
    uint32_t _arg_size;
    uint32_t _args_count;
    uint32_t _size_left;
    std::vector<bytes>  _args_list;
public:
    void init() {
        init_base();
        _state = protocol_state::error;
        _args_list.clear();
        _args_count = 0;
        _size_left = 0;
        _arg_size = 0;
        %% write init;
    }

    char* parse(char* p, char* pe, char* eof) {
        bytes_builder::guard g(_builder, p, pe);
        auto str = [this, &g, &p] {
            g.mark_end(p);
            auto s =  get_str();
            return s;
        };

        %% write exec;
        if (_state != protocol_state::error) {
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
        return _state == protocol_state::eof;
    }
};
