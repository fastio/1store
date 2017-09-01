#pragma once
#include "bytes.hh"
#include <vector>

namespace redis {

class partitioner {
    unsigned _sharding_ignore_msb_bits;
    std::vector<uint64_t> _shard_start = init_zero_based_shard_start(_shard_count, _sharding_ignore_msb_bits);
public:
    partitioner(unsigned shard_count = smp::count, unsigned sharding_ignore_msb_bits = 0)
            : i_partitioner(shard_count)
            // if one shard, ignore sharding_ignore_msb_bits as they will just cause needless
            // range breaks
            , _sharding_ignore_msb_bits(shard_count > 1 ? sharding_ignore_msb_bits : 0) {
    }
    virtual const sstring name() const { return "default"; }
    virtual int tri_compare(const token& t1, const token& t2) const override;
    virtual token midpoint(const token& t1, const token& t2) const override;
    virtual sstring to_sstring(const dht::token& t) const override;
    virtual token from_sstring(const sstring& t) const override;
    virtual token from_bytes(bytes_view bytes) const override;

    virtual unsigned shard_of(const token& t) const override;
    virtual token token_for_next_shard(const token& t, shard_id shard, unsigned spans) const override;
private:
    using uint128_t = unsigned __int128;
    static int64_t normalize(int64_t in);
    token get_token(bytes_view key);
    token get_token(uint64_t value) const;
    token bias(uint64_t value) const;      // translate from a zero-baed range
    uint64_t unbias(const token& t) const; // translate to a zero-baed range
    static unsigned zero_based_shard_of(uint64_t zero_based_token, unsigned shards, unsigned sharding_ignore_msb_bits);
    static std::vector<uint64_t> init_zero_based_shard_start(unsigned shards, unsigned sharding_ignore_msb_bits);
};


}

