#include "native_protocol_parser.hh"
#include <unordered_map>
#include "redis_command_code.hh"
#include "exceptions/exceptions.hh"
#include <cctype>
#include <cstring>
namespace redis {

static thread_local std::unordered_map<bytes, command_code> _command_table {
   { "set", command_code::set },
   { "mset", command_code::mset },
   { "get", command_code::get },
   { "mget", command_code::mget },
   { "del", command_code::del },
   { "echo", command_code::echo },
   { "ping", command_code::ping },
   { "incr", command_code::incr },
   { "decr", command_code::decr },
   { "incrby", command_code::incrby },
   { "decrby", command_code::decrby },
   { "command", command_code::command },
   { "exists", command_code::exists },
   { "append", command_code::append },
   { "strlen", command_code::strlen },
   { "lpush", command_code::lpush },
   { "lpushx", command_code::lpushx },
   { "lpop", command_code::lpop },
   { "llen", command_code::llen },
   { "lindex", command_code::lindex },
   { "linsert", command_code::linsert },
   { "lrange", command_code::lrange },
   { "lset", command_code::lset },
   { "rpush", command_code::rpush },
   { "rpushx", command_code::rpushx },
   { "rpop", command_code::rpop },
   { "lrem", command_code::lrem },
   { "ltrim", command_code::ltrim },
   { "hset", command_code::hset },
   { "hdel", command_code::hdel },
   { "hget", command_code::hget },
   { "hlen", command_code::hlen },
   { "hexists", command_code::hexists },
   { "hstrlen", command_code::hstrlen },
   { "hincrby", command_code::hincrby },
   { "hincrbyfloat", command_code::hincrbyfloat },
   { "hkeys", command_code::hkeys },
   { "hvals", command_code::hvals },
   { "hmget", command_code::hmget },
   { "hmset", command_code::hmset },
   { "hgetall", command_code::hgetall },
   { "sadd", command_code::sadd },
   { "scard", command_code::scard },
   { "sismember", command_code::sismember },
   { "smembers", command_code::smembers },
   { "srem", command_code::srem },
   { "sdiff", command_code::sdiff },
   { "sdiffstore", command_code::sdiffstore },
   { "sunion", command_code::sunion },
   { "sunionstore", command_code::sunionstore },
   { "smove", command_code::smove },
   { "srandmember", command_code::srandmember },
   { "spop", command_code::spop },
   { "type", command_code::type },
   { "expire", command_code::expire },
   { "pexpire", command_code::pexpire },
   { "ttl", command_code::ttl },
   { "pttl", command_code::pttl },
   { "persist", command_code::persist },
   { "zadd", command_code::zadd },
   { "zcard", command_code::zcard },
   { "zcount", command_code::zcount },
   { "zrange", command_code::zrange },
   { "zrangebyscore", command_code::zrangebyscore },
   { "zrank", command_code::zrank },
   { "zrem", command_code::zrem },
   { "zremrangebyrank", command_code::zremrangebyrank },
   { "zremrangebyscore", command_code::zremrangebyscore },
   { "zrevrange", command_code::zrevrange },
   { "zrevrangebyscore", command_code::zrevrangebyscore },
   { "zscore", command_code::zscore },
   { "zunionstore", command_code::zunionstore },
   { "zinterstore", command_code::zinterstore },
   { "zdiffstore", command_code::zdiffstore },
   { "zunion", command_code::zunion },
   { "zinter", command_code::zinter },
   { "zdiff", command_code::zdiff },
   { "zscan", command_code::zscan },
   { "zrangebylex", command_code::zrangebylex },
   { "zlexcount", command_code::zlexcount },
   { "zremrangebylex", command_code::zremrangebylex },
   { "select", command_code::select },
   { "geoadd", command_code::geoadd },
   { "geohash", command_code::geohash },
   { "geodist", command_code::geodist },
   { "geopos", command_code::geopos },
   { "georadius", command_code::georadius },
   { "georadiusbymember", command_code::georadiusbymember },
   { "setbit", command_code::setbit },
   { "getbit", command_code::getbit },
   { "bitcount", command_code::bitcount },
   { "bitop", command_code::bitop },
   { "bitpos", command_code::bitpos },
   { "bitfield", command_code::bitfield },
   { "pfadd", command_code::pfadd },
   { "pfcount", command_code::pfcount },
   { "pfmerge", command_code::pfmerge },
};

void native_protocol_parser::init()
{
    _req._command =  {};
    _req._state = protocol_state::error;
    _req._args_count = 0;
    _req._args.clear();
}

uint32_t native_protocol_parser::convert_to_number(char* begin, char* end) const
{
    auto num = std::atoi(begin);
    if (num == 0) {
        throw protocol_exception("Protocol error: invalid request");
    }
    return static_cast<uint32_t> (num);
}

char* native_protocol_parser::find_first_nonnumeric(char* begin, char* end) const
{
    for (auto s = begin; s < end; ++s) {
        if (!isdigit(*s)) {
            return s;
        }
    }
    return end;
}

char* native_protocol_parser::parse(char* p, char* pe, char* eof)
{
    auto s = p;
    auto begin = s, end = s;
    auto limit = pe - 1;
    bool parse_cmd_name = false;
    _req._command = {};
    while (s < limit) {
        while (*s == ' ') ++s;
        if (*s == '\r' && *(s + 1) == '\n') {
            // end with \r\n
            end = s + 2;
            if (*begin == '*') {
                if (parse_cmd_name) {
                    throw protocol_exception("Protocol error: invalid request");
                }
                parse_cmd_name = true;
                assert(_req._args.empty());
                ++begin;
                _req._args_count = convert_to_number(begin, end) - 1;
                // skip \r\n
                s += 2;
                begin = s;
                continue;
            }
            if (*begin != '$') {
                throw unexpect_protocol_exception('$', *begin);
            }
            if (!parse_cmd_name) {
                // igore the inline request format
                throw protocol_exception("Protocol error: inline request format is not supported");
            }
            ++begin;
            auto endnumber = find_first_nonnumeric(begin, end);
            if (endnumber == end) {
                throw protocol_exception("Protocol error: invalid request");
            }
            auto number = convert_to_number(begin, endnumber);
            end += number;
            end += 2;
            begin = end;
            endnumber += 2;
            if (end > pe || endnumber > pe) {
                throw protocol_exception("Protocol error: invalid request");
            }
            if (_req._command.empty()) {
                // command string
                _req._command = bytes { endnumber, number };
                s = end;
                begin = s;
                continue;
            }
            _req._args.emplace_back(bytes { endnumber, number });
            if (_req._args.size() == _req._args_count) {
                // ok
                _req._state = protocol_state::ok;
                return end;
            }
            s = end;
            continue;
        }
        ++s;
    }
    if (static_cast<size_t>(pe - p) > MAX_INLINE_BUFFER_SIZE) {
        throw protocol_exception("Protocol error: too big bulk count string");
    }
    // here, we need more data to construct request.
    return nullptr;
}
}
