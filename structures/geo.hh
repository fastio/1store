/*
* Pedis is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* You may obtain a copy of the License at
*
*     http://www.gnu.org/licenses
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
* 
*  Copyright (c) 2016-2026, Peng Jian, pstack@163.com. All rights reserved.
*
*/
#pragma once
#include "utils/bytes.hh"
namespace redis {
static constexpr const int GEODIST_UNIT_M  = (1 << 0);
static constexpr const int GEODIST_UNIT_KM = (1 << 1);
static constexpr const int GEODIST_UNIT_MI = (1 << 2);
static constexpr const int GEODIST_UNIT_FT = (1 << 3);

static constexpr const int GEORADIUS_ASC         = (1 << 0);
static constexpr const int GEORADIUS_DESC        = (1 << 1);
static constexpr const int GEORADIUS_WITHCOORD   = (1 << 2);
static constexpr const int GEORADIUS_WITHSCORE   = (1 << 3);
static constexpr const int GEORADIUS_WITHHASH    = (1 << 4);
static constexpr const int GEORADIUS_WITHDIST    = (1 << 5);
static constexpr const int GEORADIUS_COUNT       = (1 << 6);
static constexpr const int GEORADIUS_STORE_SCORE = (1 << 7);
static constexpr const int GEORADIUS_STORE_DIST  = (1 << 8);
static constexpr const int GEO_UNIT_M      = (1 << 9);
static constexpr const int GEO_UNIT_KM     = (1 << 10);
static constexpr const int GEO_UNIT_MI     = (1 << 11);
static constexpr const int GEO_UNIT_FT     = (1 << 12);
class geo {
public:
    static bool encode_to_geohash(const double& longitude, const double& latitude, double& geohash);
    static bool encode_to_geohash_string(const double& geohash, bytes& geohashstr);
    static bool decode_from_geohash(const double& geohash, double& longitude, double& latitude);
    static bool dist(const double& lscore, const double& rscore, double& line);
    static bool dist(const double& llongitude, const double& llatitude, const double& rlongtitude, const double& rlatitude, double& line);
    static bytes to_bytes(const long long& u);

    //[key, dist, score, longitude, latitude]
    using points_type = std::vector<std::tuple<bytes, double, double, double, double>>;
    using fetch_point = std::function<size_t (uint64_t, uint64_t, const double, const double, const double, points_type& points)>;
    static bool fetch_points_from_location(double longitude, double latitude, double radius, fetch_point&& f, points_type& points);
    static bool to_meters(double& n, int flags);
    static bool from_meters(double& n, int flags);
};

}
