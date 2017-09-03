#pragma once
#include <stdexcept>
#include "core/sstring.hh"
#include "core/print.hh"
#include "utils/bytes.hh"

namespace redis {

enum class exception_code : int32_t {
    SERVER_ERROR    = 0x0000,
    PROTOCOL_ERROR  = 0x000A,

    // 3xx: io error
    IO_ERROR    = 0x3000,
};

class io_exception : public std::exception {
private:
    exception_code _code;
    sstring _msg;
protected:
    template<typename... Args>
    static inline sstring prepare_message(const char* fmt, Args&&... args) noexcept {
        try {
            return sprint(fmt, std::forward<Args>(args)...);
        } catch (...) {
            return sstring();
        }
    }
public:
    io_exception(sstring msg) noexcept
        : _code(exception_code::IO_ERROR)
        , _msg(std::move(msg))
    { }
    virtual const char* what() const noexcept override { return _msg.begin(); }
    exception_code code() const { return _code; }
    sstring get_message() const { return what(); }
};

}
