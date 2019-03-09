#pragma once
#include "db/config.hh"
#include "seastar/core/future.hh"
#include "seastar/core/shared_ptr.hh"

using namespace seastar;
namespace redis {

static constexpr auto NAME = "redis";
static constexpr auto SIMPLE_OBJECTS = "simple_objects";
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
