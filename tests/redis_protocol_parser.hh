#pragma once
#include <algorithm>
#include <memory>
#include <cassert>
#include <cstring>
#include <experimental/optional>
#include "seastar/core/future.hh"
#include "redis/request.hh"
#include "redis/redis_command_code.hh"

class protocol_exception : public std::exception {
    sstring _msg;
public:
    protocol_exception(sstring msg) : _msg(msg) {}
    ~protocol_exception() {}
    virtual const char* what() const noexcept override { return _msg.begin(); }
};
class redis_protocol_parser {
    static constexpr size_t MAX_INLINE_BUFFER_SIZE = 1024 * 64; // 64K
    const char* find_first_nonnumeric(const char* begin, const char* end) const {
        for (auto s = begin; s < end; ++s) {
            if (!isdigit(*s)) {
                return s;
            }
        }
        return end;
    }
    uint32_t convert_to_number(const char* begin, const char* end) const {
        auto num = std::atoi(begin);
        if (num == 0) {
            throw protocol_exception("Protocol error: invalid request");
        }
        return static_cast<uint32_t> (num);
    }
    const char* parse_as_request_impl(const char* p, const char* pe, const char* eof, redis::request& req) {
        req._command =  {};
        req._state = redis::protocol_state::error;
        req._args_count = 0;
        req._args.clear();
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
                        throw protocol_exception("Protocol error: invalid request");
                    }
                    parse_cmd_name = true;
                    assert(req._args.empty());
                    ++begin;
                    req._args_count = convert_to_number(begin, end) - 1;
                    // skip \r\n
                    s += 2;
                    begin = s;
                    continue;
                }
                if (*begin != '$') {
                    throw protocol_exception("Protocol error: $ is execpected");
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
                if (req._command.empty()) {
                    // command string
                    req._command = bytes { reinterpret_cast<const int8_t*>(endnumber), static_cast<size_t>(number) };
                    s = end;
                    begin = s;
                    continue;
                }
                req._args.emplace_back(bytes { reinterpret_cast<const int8_t*>(endnumber), static_cast<size_t>(number) });
                if (req._args.size() == req._args_count) {
                    // ok
                    req._state = redis::protocol_state::ok;
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
public:
    redis_protocol_parser() {}
    ~redis_protocol_parser() {}

    redis::request&& parse_as_request(const sstring& b) {
        redis::request req;
        parse_as_request_impl(b.data(), b.data() + b.size(), nullptr, req);
        return std::move(req);
    }
};

inline std::unique_ptr<redis_protocol_parser> make_redis_parser() {
    return std::make_unique<redis_protocol_parser>();
}
