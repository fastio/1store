#pragma once
#include "utils/bytes.hh"
#include "core/reactor.hh"
#include "token.hh"
#include "utils/managed_bytes.hh"
namespace redis {
struct decorated_key {
    managed_bytes _key;
    token _token;
    decorated_key() = default;
    decorated_key(managed_bytes&& mb, token&& t) : _key(std::move(mb)), _token(t) {}
    decorated_key(decorated_key&& o) : _key(std::move(o._key)), _token(std::move(o._token)) {}
    decorated_key(const decorated_key& o) : _key(o._key), _token(o._token) {}
    decorated_key& operator = (decorated_key&& o) {
        if (this != &o) {
            _key = std::move(o._key);
            _token = std::move(o._token);
        }
        return *this;
    }
    decorated_key& operator = (const decorated_key& o) {
        if (this != &o) {
            _key = o._key;
            _token = o._token;
        }
        return *this;
    }
    bytes_view key_view() const {
        return bytes_view { _key };
    }
    friend bool operator == (const decorated_key& l, const decorated_key& r);
};

struct redis_key {
    bytes _key;
    size_t _hash;
    redis_key(bytes key) : _key(std::move(key)), _hash(std::hash<bytes>()(_key)) {}
    redis_key& operator = (const redis_key& o) {
        if (this != &o) {
            _key = o._key;
            _hash = o._hash;
        }
        return *this;
    }
    inline unsigned get_cpu() const { return _hash % smp::count; }
    inline const size_t hash() const { return _hash; }
    inline const bytes& key() const { return _key; }
    inline const uint32_t size() const { return _key.size(); }
    inline const char* data() const { return _key.c_str(); }
};

decorated_key to_decorated_key(const redis_key& rk);
}
