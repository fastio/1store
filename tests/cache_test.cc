#include "tests/test-utils.hh"
#include "cache.hh"

using namespace redis;

class cache_holder : private logalloc::region {
public:
    future<> insert() {
        sstring key {"redis"}, val {"test"};
        redis_key rk { std::ref(key) };

        with_allocator(allocator(), [this, &rk, &val] {
            auto entry = current_allocator().construct<cache_entry>(rk.key(), rk.hash(), val);
            _c.insert(entry);
            BOOST_CHECK(_c.size() == 1);
            BOOST_CHECK(!_c.empty());
        }); 

        _c.with_entry_run(rk, [&val, &rk] (const cache_entry* e) {
             BOOST_REQUIRE(e != nullptr);
             BOOST_CHECK(cache_entry::compare()(rk, *e) == true);
             BOOST_CHECK(e->value_bytes_size() == val.size());
             BOOST_CHECK(memcmp(e->value_bytes_data(), val.data(), val.size()) == 0);
        });

        sstring key2 {"not-exists"};
        redis_key rk2 { std::ref(key2) };
        _c.with_entry_run(rk2, [] (const cache_entry* e) {
             BOOST_REQUIRE(e == nullptr);
        });

        BOOST_CHECK(_c.erase(rk));
        BOOST_CHECK(_c.erase(rk2));

        BOOST_CHECK(_c.size() == 0);
        BOOST_CHECK(_c.empty());
        return make_ready_future<>();
    };
protected:
    cache _c;
};
SEASTAR_TEST_CASE(insert) {
    return cache_holder().insert();
}
