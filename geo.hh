#pragma once
#include "core/sstring.hh"
namespace redis {
class geo {
public:
    static bool encode_to_geohash(const double& longtitude, const double& latitude, double& geohash);
    static bool encode_to_geohash_string(const double& geohash, sstring& geohashstr);
    static bool decode_from_geohash(const double& geohash, double& longtitude, double& latitude);
    static bool dist(const double& lscore, const double& rscore, double& line);
    static sstring to_sstring(const long long& u);
};
}
