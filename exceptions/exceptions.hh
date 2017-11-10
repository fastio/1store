#pragma once
#include <stdexcept>
#include "core/sstring.hh"
#include "core/print.hh"
#include "utils/bytes.hh"

enum class exception_code : int32_t {
    SERVER_ERROR    = 0x0000,
    PROTOCOL_ERROR  = 0x000A,

    IO_ERROR    = 0x3000,
    REQUEST_ERROR    = 0x3001,
};

class base_exception : public std::exception {
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
    base_exception(exception_code code, sstring msg) noexcept
        : _code(code)
        , _msg(std::move(msg))
    { }
    virtual const char* what() const noexcept override { return _msg.begin(); }
    exception_code code() const { return _code; }
    sstring get_message() const { return what(); }
};

class io_exception : public base_exception {
public:
    io_exception(sstring msg) : base_exception(exception_code::IO_ERROR, std::move(msg)) {}
};

class request_exception : public base_exception {
public:
    request_exception(sstring msg) : base_exception(exception_code::REQUEST_ERROR, std::move(msg)) {}
};

class protocol_exception : base_exception {
public:
    protocol_exception(sstring msg) : base_exception(exception_code::REQUEST_ERROR, std::move(msg)) {}
};
