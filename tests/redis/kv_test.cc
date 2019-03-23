#include <boost/test/unit_test.hpp>
#include <seastar/core/future.hh>

#include "seastarx.hh"
#include "tests/test-utils.hh"
#include "utils/hash.hh"

#include "tests/cql_test_env.hh"

SEASTAR_TEST_CASE(test_pair_hash){
    return do_with_cql_env_thread([] (auto& e) {
        return make_ready_future<>();
    });
}
