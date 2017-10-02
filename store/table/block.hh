#pragma once
#include <stddef.h>
#include <stdint.h>
#include "utils/managed_bytes.hh"
#include "core/temporary_buffer.hh"
#include "lru_cache.hh"
#include "seastarx.hh"

namespace store {

class block_cache;

class block {
    managed_bytes _cache_key;     // used to lookup
    managed_bytes _data; 
    uint32_t _size;
    uint32_t _restart_offset;     // Offset in data_ of restart array
    uint32_t num_restarts() const;
public:
    block(managed_bytes cache_key, temporary_buffer<char> data);
    block(bytes cache_key, temporary_buffer<char> data) 
        : block(managed_bytes { bytes_view {cache_key.data(), cache_key.size() } }, std::move(data))
    {
    }

    block(block&& o)
        : _cache_key(std::move(o._cache_key))
        , _data(std::move(o._data))
        , _size(o._size)
        , _restart_offset(std::move(o._restart_offset))
    {
    }
    block& operator = (block&& o)
    {
        if (this != &o) {
            _cache_key = std::move(o._cache_key);
            _data = std::move(o._data);
            _size = o._size;
            _restart_offset = std::move(o._restart_offset);
        }
        return *this;
    }

    ~block() {}

    size_t size() const { return _size; }
    friend class block_cache;
};

class block_cache : public redis::base_cache<managed_bytes, lw_shared_ptr<block>> { 
public:
    block_cache()
        : redis::base_cache<managed_bytes, lw_shared_ptr<block>> (redis::global_cache_tracker<managed_bytes, lw_shared_ptr<block>>())
    {
    }
    block_cache(block_cache&&) = delete;
    block_cache(const block_cache&) = delete;
    block_cache& operator = (const block_cache&) = delete;
    block_cache& operator = (block_cache&&) = delete;
};

}
