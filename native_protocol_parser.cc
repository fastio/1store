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
};

void native_protocol_parser::init()
{
    _req._command = command_code::unknown;
    _req._args_count = 0;
    _req._args.clear();
}

uint32_t native_protocol_parser::convert_to_number(char* begin, char* end)
{
    auto num = std::atoi(begin);
    if (num == 0) {
    }
    return static_cast<uint32_t> (num);
}

char* native_protocol_parser::find_first_nonnumeric(char* begin, char* end)
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
    while (s < limit) {
        while (*s == ' ') ++s;
        if (*s == '\r' && *(s + 1) == '\n') {
            // end with \r\n
            end = s + 2;
            if (*begin == '*') {
                if (_unfinished) {
                    // protocol error, throw exception
                }
                _unfinished = true;
                assert(_req._args.empty());
                ++begin;
                _req._args_count = convert_to_number(begin, end) - 1;
                // skip \r\n
                s += 2;
                begin = s;
                continue;
            }
            if (*begin != '$') {
                // protocol error
            }
            ++begin;
            auto endnumber = find_first_nonnumeric(begin, end);
            if (endnumber == end) {
                // protocol error
            }
            auto number = convert_to_number(begin, endnumber);
            if (number != static_cast<uint32_t>(end - endnumber)) {
                // protocol error
            }
            end += number;
            end += 2;
            begin = end;
            endnumber += 2;
            if (_req._command == command_code::unknown) {
                // command string
                bytes command { endnumber, number};
                auto c = _command_table.find(command);
                if (c != _command_table.end()) {
                    _req._command = c->second;
                    s = end; 
                    begin = s;
                    continue;
                }
                // protocol error
            }
            _req._args.emplace_back(bytes { endnumber, number });
            if (_req._args.size() == _req._args_count) {
                // ok
                _unfinished = false;
                _req._state = protocol_state::ok; 
                return end;
            }
            s = end;
            continue;
        }
        ++s;
    }
    if (_req._args.size() == _req._args_count) {
        // ok
        _unfinished = false;
        _req._state = protocol_state::ok; 
        return end;
    }
    if (begin != p) _unfinished = true;
    return begin;
}
}
