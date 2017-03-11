#pragma once
#include "core/sstring.hh"
namespace redis {
//struct std::tuple<sstring, double, double, double, double>;
class geo {
public:
    struct geo_hash
    {
        uint64_t _hash;
        uint8_t _step;
        geo_hash() : _hash(0), _step(0) {}
    };
    struct geo_hash_range
    {
        double _min;
        double _max;
        geo_hash_range() : _min(0), _max(0) {}
    };
    struct geo_hash_area
    {
        geo_hash _hash;
        geo_hash_range _longitude_range;
        geo_hash_range _latitude_range;
        geo_hash_area() : _hash(), _longitude_range(), _latitude_range() {}
    };
    struct geo_hash_neighbors {
        geo_hash _north;
        geo_hash _east;
        geo_hash _west;
        geo_hash _south;
        geo_hash _north_east;
        geo_hash _south_east;
        geo_hash _north_west;
        geo_hash _south_west;
        geo_hash_neighbors() : _north(), _east(), _west(), _south(), _north_east(), _south_east(), _north_west(), _south_west() {}
    };
    struct geo_radius
    {
        geo_hash _hash;
        geo_hash_area _area;
        geo_hash_neighbors _neighbors;
        geo_radius() : _hash(), _area(), _neighbors() {}
    };

    static bool encode_to_geohash(const double& longitude, const double& latitude, double& geohash);
    static bool encode_to_geohash_string(const double& geohash, sstring& geohashstr);
    static bool decode_from_geohash(const double& geohash, double& longitude, double& latitude);
    static bool dist(const double& lscore, const double& rscore, double& line);
    static bool dist(const double& llongitude, const double& llatitude, const double& rlongtitude, const double& rlatitude, double& line);
    static sstring to_sstring(const long long& u);

    //[key, dist, score, longitude, latitude]
    using points_type = std::vector<std::tuple<sstring, double, double, double, double>>;
    using fetch_point = std::function<size_t (uint64_t, uint64_t, const double, const double, const double, points_type& points)>;
    static bool fetch_points_from_location(double longitude, double latitude, double radius, fetch_point&& f, points_type& points);
    static bool to_meters(double& n, int flags);
    static bool from_meters(double& n, int flags);
};

}
