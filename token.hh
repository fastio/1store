#pragma once
#include "utils/managed_bytes.hh"
namespace redis {
class token {
public:
    enum class kind {
        before_all_keys,
        key,
        after_all_keys,
    };
    kind _kind;
    // _data can be interpreted as a big endian binary fraction
    // in the range [0.0, 1.0).
    //
    // So, [] == 0.0
    //     [0x00] == 0.0
    //     [0x80] == 0.5
    //     [0x00, 0x80] == 1/512
    //     [0xff, 0x80] == 1 - 1/512
    managed_bytes _data;

    token() : _kind(kind::before_all_keys) {
    }

    token(kind k, managed_bytes d) : _kind(std::move(k)), _data(std::move(d)) {
    }

    bool is_minimum() const {
        return _kind == kind::before_all_keys;
    }

    bool is_maximum() const {
        return _kind == kind::after_all_keys;
    }

    static token from_bytes(const bytes& key);
    static token from_bytes(const bytes_view& key);
};
token midpoint_unsigned(const token& t1, const token& t2);
const token& minimum_token();
const token& maximum_token();
bool operator==(const token& t1, const token& t2);
bool operator<(const token& t1, const token& t2);
int tri_compare(const token& t1, const token& t2);
inline bool operator!=(const token& t1, const token& t2) { return std::rel_ops::operator!=(t1, t2); }
inline bool operator>(const token& t1, const token& t2) { return std::rel_ops::operator>(t1, t2); }
inline bool operator<=(const token& t1, const token& t2) { return std::rel_ops::operator<=(t1, t2); }
inline bool operator>=(const token& t1, const token& t2) { return std::rel_ops::operator>=(t1, t2); }
std::ostream& operator<<(std::ostream& out, const token& t);
}

