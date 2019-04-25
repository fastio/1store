#include <boost/test/unit_test.hpp>
#include <seastar/core/future.hh>

#include "seastarx.hh"
#include "tests/test-utils.hh"
#include "utils/hash.hh"

#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"
#include "redis/redis_keyspace.hh"

SEASTAR_TEST_CASE(test_redis_set) {
    return do_with_redis_env_thread([] (auto& e) {
        auto&& reply = e.execute_redis("set a b").get0();
        assert_that(std::move(reply)).is_redis_reply()
            .with_status(bytes("OK"));

        auto msg = e.execute_cql(sprint("select * from redis.%s where pkey = \'a\'", redis::STRINGS)).get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({
                {bytes_type->decompose(data_value(bytes("a")))},
                {bytes_type->decompose(data_value(bytes("b")))},
            });
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_redis_set_3_args) {
    return do_with_redis_env_thread([] (auto& e) {
        auto&& reply = e.execute_redis("set a1 b c").get0();
        assert_that(std::move(reply)).is_redis_reply()
            .with_error(bytes("ERR syntax error"));

        auto msg = e.execute_cql(sprint("select * from redis.%s where pkey = \'a1\'", redis::STRINGS)).get0();
        assert_that(msg).is_rows()
            .is_empty();
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_redis_get) {
    return do_with_redis_env_thread([] (auto& e) {
        auto&& reply = e.execute_redis("set a b123456789").get0();
        assert_that(std::move(reply)).is_redis_reply()
            .with_status(bytes("OK"));

        auto msg = e.execute_cql(sprint("select * from redis.%s where pkey = \'a\'", redis::STRINGS)).get0();
        assert_that(msg).is_rows()
            .with_size(1)
            .with_row({
                {bytes_type->decompose(data_value(bytes("a")))},
                {bytes_type->decompose(data_value(bytes("b123456789")))},
            });

        auto&& get_reply = e.execute_redis("get a").get0();
        assert_that(std::move(get_reply)).is_redis_reply()
            .with_bulk(bytes("b123456789"));
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_redis_get_wrong_args_0) {
    return do_with_redis_env_thread([] (auto& e) {
        auto&& get_reply = e.execute_redis("get").get0();
        assert_that(std::move(get_reply)).is_redis_reply()
            .with_error(bytes("wrong number of arguments (given 0, expected 1)"));
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_redis_get_wrong_args_3) {
    return do_with_redis_env_thread([] (auto& e) {
        auto&& get_reply = e.execute_redis("get a b c").get0();
        assert_that(std::move(get_reply)).is_redis_reply()
            .with_error(bytes("wrong number of arguments (given 3, expected 1)"));
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(test_redis_get_not_exists) {
    return do_with_redis_env_thread([] (auto& e) {
        auto&& get_reply = e.execute_redis("get a").get0();
        assert_that(std::move(get_reply)).is_redis_reply().is_empty();
        return make_ready_future<>();
    });
}
