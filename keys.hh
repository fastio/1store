#pragma once
#include "utils/bytes.hh"
#include "core/reactor.hh"
namespace redis {
struct redis_key {
    bytes _key;
    size_t _hash;
    redis_key(bytes& key) : _key(key), _hash(std::hash<bytes>()(_key)) {}
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
}
