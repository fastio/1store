#include <boost/test/unit_test.hpp>
#include <seastar/core/future.hh>

#include "seastarx.hh"
#include "tests/test-utils.hh"
#include "utils/hash.hh"

#include "tests/cql_test_env.hh"

SEASTAR_TEST_CASE(test_redis_set){
    return do_with_redis_env_thread([] (auto& e) {
        return e.execute_redis_command("set a b").then([] (auto) {
            return make_ready_future<>();
        });
    });
}
