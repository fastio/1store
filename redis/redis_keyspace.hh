#pragma once
#include "db/config.hh"
#include "seastar/core/future.hh"
#include "seastar/core/shared_ptr.hh"
#include "seastar/core/sharded.hh"
using namespace seastar;
namespace auth {
class service;
}
namespace service {
class client_state;
}
namespace redis {

static constexpr auto REDIS_DATABASE_NAME_PREFIX = "redis_";
static constexpr auto DEFAULT_DATABASE_NAME = "redis_0";
static constexpr auto STRINGS = "strings";
static constexpr auto LISTS = "lists";
static constexpr auto SETS = "sets";
static constexpr auto MAPS = "maps";
static constexpr auto ZSETS = "zsets";
static constexpr auto DATA_COLUMN_NAME = "data";
static constexpr auto PKEY_COLUMN_NAME = "pkey";
static constexpr auto CKEY_COLUMN_NAME = "ckey";

class redis_keyspace_helper {
public:
static future<> create_if_not_exists(lw_shared_ptr<db::config> config);
};
}
