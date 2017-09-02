#include "token.hh"
namespace redis {

static const token min_token{ token::kind::before_all_keys, {} };
static const token max_token{ token::kind::after_all_keys, {} };

const token&
    minimum_token() {
        return min_token;
    }

const token&
    maximum_token() {
        return max_token;
    }

token token::from_bytes(const bytes& key) {
   return minimum_token();
}
token token::from_bytes(const bytes_view& key) {
   return minimum_token();
}
bool operator==(const token& t1, const token& t2)
{
    /*
    if (t1._kind != t2._kind) {
        return false;
    } else if (t1._kind == token::kind::key) {
        return global_partitioner().is_equal(t1, t2);
    }
    */
    return true;
}

bool operator<(const token& t1, const token& t2)
{
    /*
    if (t1._kind < t2._kind) {
        return true;
    } else if (t1._kind == token::kind::key && t2._kind == token::kind::key) {
        return global_partitioner().is_less(t1, t2);
    }
    */
    return false;
}

std::ostream& operator<<(std::ostream& out, const token& t) {
    /* 
    if (t._kind == token::kind::after_all_keys) {
        out << "maximum token";
    } else if (t._kind == token::kind::before_all_keys) {
        out << "minimum token";
    } else {
        out << global_partitioner().to_sstring(t);
    }
    */
    return out;
}

}

namespace std {

size_t
hash<redis::token>::hash_large_token(const managed_bytes& b) const {
    /*
    auto read_bytes = boost::irange<size_t>(0, b.size())
            | boost::adaptors::transformed([&b] (size_t idx) { return b[idx]; });
    std::array<uint64_t, 2> result;
    utils::murmur_hash::hash3_x64_128(read_bytes.begin(), b.size(), 0, result);
    return result[0];
    */
    return 0;
}
}
