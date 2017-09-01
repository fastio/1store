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
}
token token::from_bytes(const bytes_view& key) {
}
}
