#include "native_protocol_parser.hh"
#include <unordered_map>
#include "redis_command_code.hh"
#include "exceptions/exceptions.hh"
#include <cctype>
#include <cstring>
namespace redis {

void native_protocol_parser::init()
{
    _req._command = {};
    _req._state = protocol_state::error;
    _req._args_count = 0;
    _req._args.clear();
}

uint32_t native_protocol_parser::convert_to_number(char* begin, char* end) const
{
    auto num = std::atoi(begin);
    if (num == 0) {
        throw exceptions::protocol_exception("Protocol error: invalid request");
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
    while (s < limit) {
        while (*s == ' ') ++s;
        if (*s == '\r' && *(s + 1) == '\n') {
            // end with \r\n
            end = s + 2;
            if (*begin == '*') {
                if (parse_cmd_name) {
                    throw exceptions::protocol_exception("Protocol error: invalid request");
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
                throw exceptions::protocol_exception("Protocol error: unexpected $");
            }
            if (!parse_cmd_name) {
                // igore the inline request format
                throw exceptions::protocol_exception("Protocol error: inline request format is not supported");
            }
            ++begin;
            auto endnumber = find_first_nonnumeric(begin, end);
            if (endnumber == end) {
                throw exceptions::protocol_exception("Protocol error: invalid request");
            }
            auto number = convert_to_number(begin, endnumber);
            end += number;
            end += 2;
            begin = end;
            endnumber += 2;
            if (end > pe || endnumber > pe) {
                throw exceptions::protocol_exception("Protocol error: invalid request");
            }
            if (_req._command.empty()) {
                // command string
                _req._command = std::move(bytes {endnumber, endnumber + number}); 
                s = end;
                begin = s;
                continue;
            }
            _req._args.emplace_back(bytes { endnumber, endnumber + number });
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
        throw exceptions::protocol_exception("Protocol error: too big bulk count string");
    }
    // here, we need more data to construct request.
    return nullptr;
}
}
