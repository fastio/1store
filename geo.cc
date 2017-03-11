#include "geo.hh"
#include "base.hh"
namespace redis {
static inline uint64_t interleave64(uint32_t xlo, uint32_t ylo) {
    static const uint64_t B[] = {
        0x5555555555555555ULL,
        0x3333333333333333ULL,
        0x0F0F0F0F0F0F0F0FULL,
        0x00FF00FF00FF00FFULL,
        0x0000FFFF0000FFFFULL
    };
    static const unsigned int S[] = {1, 2, 4, 8, 16};

    uint64_t x = xlo;
    uint64_t y = ylo;

    x = (x | (x << S[4])) & B[4];
    y = (y | (y << S[4])) & B[4];

    x = (x | (x << S[3])) & B[3];
    y = (y | (y << S[3])) & B[3];

    x = (x | (x << S[2])) & B[2];
    y = (y | (y << S[2])) & B[2];

    x = (x | (x << S[1])) & B[1];
    y = (y | (y << S[1])) & B[1];

    x = (x | (x << S[0])) & B[0];
    y = (y | (y << S[0])) & B[0];

    return x | (y << 1);
}

static inline uint64_t deinterleave64(uint64_t interleaved) {
    static const uint64_t B[] = {
        0x5555555555555555ULL,
        0x3333333333333333ULL,
        0x0F0F0F0F0F0F0F0FULL,
        0x00FF00FF00FF00FFULL,
        0x0000FFFF0000FFFFULL,
        0x00000000FFFFFFFFULL
    };
    static const unsigned int S[] = {0, 1, 2, 4, 8, 16};

    uint64_t x = interleaved;
    uint64_t y = interleaved >> 1;

    x = (x | (x >> S[0])) & B[0];
    y = (y | (y >> S[0])) & B[0];

    x = (x | (x >> S[1])) & B[1];
    y = (y | (y >> S[1])) & B[1];

    x = (x | (x >> S[2])) & B[2];
    y = (y | (y >> S[2])) & B[2];

    x = (x | (x >> S[3])) & B[3];
    y = (y | (y >> S[3])) & B[3];

    x = (x | (x >> S[4])) & B[4];
    y = (y | (y >> S[4])) & B[4];

    x = (x | (x >> S[5])) & B[5];
    y = (y | (y >> S[5])) & B[5];

    return x | (y << 32);
}
sstring geo::to_sstring(const long long& u) {
    char s[21];
    size_t l;
    char *p, aux;
    unsigned long long v;
    v = (u < 0) ? -u : u;
    p = s;
    do {
        *p++ = '0'+(v%10);
        v /= 10;
    } while(v);
    if (u < 0) *p++ = '-';

    l = p-s;
    *p = '\0';

    p--;
    char* ps = s;
    while(ps < p) {
        aux = *ps;
        *ps = *p;
        *p = aux;
        ps++;
        p--;
    }
    return std::move(sstring(s, l));
}

bool geo::encode_to_geohash(const double& longitude, const double& latitude, double& geohash)
{
    if (longitude > GEO_LONG_MAX || longitude < GEO_LONG_MIN || latitude > GEO_LAT_MAX || latitude < GEO_LAT_MIN) {
        return false;
    }

    uint64_t bits = 0;
    double lat_offset = (latitude - GEO_LAT_MIN) / (GEO_LAT_SCALE);
    double long_offset = (longitude - GEO_LONG_MIN) / (GEO_LONG_SCALE);

    lat_offset *= (1 << GEO_HASH_STEP_MAX);
    long_offset *= (1 << GEO_HASH_STEP_MAX);
    bits = interleave64(lat_offset, long_offset);

    bits <<= (52 - GEO_HASH_STEP_MAX * 2);
    auto bits_str = to_sstring(bits);
    try {
        geohash = std::stod(bits_str.c_str());
    } catch (const std::invalid_argument&) {
        return false;
    }
    return true;
}

bool geo::encode_to_geohash_string(const double& score, sstring& geohashstr)
{
    static const char* ALPHABET = "0123456789bcdefghjkmnpqrstuvwxyz";
    double longitude = 0, latitude = 0;
    if (decode_from_geohash(score, longitude, latitude) == false) {
        return false;
    }
    if (longitude > GEO_LONG_MAX || longitude < GEO_LONG_MIN || latitude > GEO_LAT_MAX_STD || latitude < GEO_LAT_MIN_STD) {
        return false;
    }
    uint64_t bits = 0;
    double lat_offset =
        (latitude - GEO_LAT_MIN_STD) / (GEO_LAT_STD_SCALE);
    double long_offset =
        (longitude - GEO_LONG_MIN) / (GEO_LONG_SCALE);

    lat_offset *= (1 << GEO_HASH_STEP_MAX);
    long_offset *= (1 << GEO_HASH_STEP_MAX);
    bits = interleave64(lat_offset, long_offset);

    bits <<= (52 - GEO_HASH_STEP_MAX * 2);

    char b[12];
    for (int i = 0; i < 11; i++) {
        int idx = (bits >> (52-((i+1)*5))) & 0x1f;
        b[i] = ALPHABET[idx];
    }
    b[11] = '\0';
    geohashstr = std::move(sstring(b, 11));
    return true;
}

bool geo::decode_from_geohash(const double& geohash, double& longitude, double& latitude)
{
    uint64_t hash_sep = deinterleave64((uint64_t)geohash); /* hash = [LAT][LONG] */
    uint32_t ilato = hash_sep;
    uint32_t ilono = hash_sep >> 32;
    double latitude_min =  GEO_LAT_MIN + (ilato * 1.0 / (1ull << GEO_HASH_STEP_MAX)) * GEO_LAT_SCALE;
    double latitude_max =  GEO_LAT_MIN + ((ilato + 1) * 1.0 / (1ull << GEO_HASH_STEP_MAX)) * GEO_LAT_SCALE;
    double longitude_min = GEO_LONG_MIN + (ilono * 1.0 / (1ull << GEO_HASH_STEP_MAX)) * GEO_LONG_SCALE;
    double longitude_max = GEO_LONG_MIN + ((ilono + 1) * 1.0 / (1ull << GEO_HASH_STEP_MAX)) * GEO_LONG_SCALE;
    longitude = (longitude_min + longitude_max) / 2.0;
    latitude = (latitude_min + latitude_max) / 2.0;
    return true;
}

static inline double deg_rad(double u)
{
    static double const pi = std::acos(-1);
    return u * (pi / 180.0);
}

static inline double rad_deg(double ang)
{
    static double const pi = std::acos(-1);
    return ang / (pi / 180.0);
}

bool geo::dist(const double& llongitude, const double& llatitude, const double& rlongitude, const double& rlatitude, double& line)
{
    double lat1r = 0, lon1r = 0, lat2r = 0, lon2r = 0, u = 0, v = 0;
    lat1r = deg_rad(llatitude);
    lon1r = deg_rad(llongitude);
    lat2r = deg_rad(rlatitude);
    lon2r = deg_rad(rlongitude);
    u = std::sin((lat2r - lat1r) / 2);
    v = std::sin((lon2r - lon1r) / 2);
    line = 2.0 * EARTH_RADIUS_IN_METERS * std::asin(std::sqrt(u * u + std::cos(lat1r) * std::cos(lat2r) * v * v));
    return true;
}

bool geo::dist(const double& lhash, const double& rhash, double& output)
{
    double llongitude = 0, llatitude = 0, rlongitude = 0, rlatitude = 0;
    if (decode_from_geohash(lhash, llongitude, llatitude) == false || decode_from_geohash(rhash, rlongitude, rlatitude) == false) {
        return false;
    }
    return dist(llongitude, llatitude, rlongitude, rlatitude, output);
}

static bool geohash_bounding_box(double longitude, double latitude, double radius, double* bounds)
{
    if (!bounds) return false;

    double lonr = deg_rad(longitude), latr = deg_rad(latitude);
    if (radius > EARTH_RADIUS_IN_METERS)
        radius = EARTH_RADIUS_IN_METERS;
    double distance = radius / EARTH_RADIUS_IN_METERS;
    double min_latitude = latr - distance;
    double max_latitude = latr + distance;

    double min_longitude, max_longitude;
    double difference_longitude = std::asin(std::sin(distance) / std::cos(latr));
    min_longitude = lonr - difference_longitude;
    max_longitude = lonr + difference_longitude;

    bounds[0] = rad_deg(min_longitude);
    bounds[1] = rad_deg(min_latitude);
    bounds[2] = rad_deg(max_longitude);
    bounds[3] = rad_deg(max_latitude);
    return true;
}

static uint8_t geohash_estimate_steps_by_radius(double range, const double lat)
{
    if (range == 0) return 26;
    int step = 1;
    while (range < MERCATOR_MAX) {
        range *= 2;
        step++;
    }
    step -= 2;
    if (lat > 67 || lat < -67) step--;
    if (lat > 80 || lat < -80) step--;

    if (step < 1) step = 1;
    if (step > 26) step = 26;
    return step;
}

static bool geohash_encode_internal(const double* bounds, const double longitude, const double latitude, uint8_t step, uint64_t& hash)
{
    if (step > 32 || step == 0 || bounds[0] == 0 || bounds[1] == 0 || bounds[2] == 0 || bounds[3] == 0) {
        return false;
    }

    if (longitude > 180 || longitude < -180 || latitude > 85.05112878 || latitude < -85.05112878) {
        return false;
    }

    if (latitude < bounds[2] || latitude > bounds[3] ||  longitude < bounds[0] || longitude > bounds[1]) {
        return false;
    }

    double lat_offset =  (latitude - bounds[2]) / (bounds[3] - bounds[2]);
    double long_offset = (longitude - bounds[0]) / (bounds[1] - bounds[0]);

    lat_offset *= (1 << step);
    long_offset *= (1 << step);
    hash = interleave64(lat_offset, long_offset);
    return true;
}

static bool geohash_decode_internal(double* bounds, geo::geo_radius& output)
{
    if (output._hash._hash == 0) {
        return false;
    }

    output._area._hash._hash = output._hash._hash;
    output._area._hash._step = output._hash._step;

    uint8_t step = output._hash._step;
    uint64_t hash_sep = deinterleave64(output._hash._hash); /* hash = [LAT][LONG] */

    double lat_scale  = bounds[3] - bounds[2];
    double long_scale = bounds[1] - bounds[0];

    uint32_t ilato = hash_sep;
    uint32_t ilono = hash_sep >> 32;

     output._area._latitude_range._min  = bounds[2] + (ilato * 1.0 / (1ull << step)) * lat_scale;
     output._area._latitude_range._max  = bounds[2] + ((ilato + 1) * 1.0 / (1ull << step)) * lat_scale;
     output._area._longitude_range._min = bounds[0] + (ilono * 1.0 / (1ull << step)) * long_scale;
     output._area._longitude_range._max = bounds[0] + ((ilono + 1) * 1.0 / (1ull << step)) * long_scale;

    return true;
}

static void geohash_move_x(geo::geo_hash& hash, int8_t d) {
    if (d == 0)
        return;

    uint64_t x = hash._hash & 0xaaaaaaaaaaaaaaaaULL;
    uint64_t y = hash._hash & 0x5555555555555555ULL;

    uint64_t zz = 0x5555555555555555ULL >> (64 - hash._step * 2);

    if (d > 0) {
        x = x + (zz + 1);
    } else {
        x = x | zz;
        x = x - (zz + 1);
    }

    x &= (0xaaaaaaaaaaaaaaaaULL >> (64 - hash._step * 2));
    hash._hash = (x | y);
}

static void geohash_move_y(geo::geo_hash& hash, int8_t d) {
    if (d == 0)
        return;

    uint64_t x = hash._hash & 0xaaaaaaaaaaaaaaaaULL;
    uint64_t y = hash._hash & 0x5555555555555555ULL;

    uint64_t zz = 0xaaaaaaaaaaaaaaaaULL >> (64 - hash._step * 2);
    if (d > 0) {
        y = y + (zz + 1);
    } else {
        y = y | zz;
        y = y - (zz + 1);
    }
    y &= (0x5555555555555555ULL >> (64 - hash._step * 2));
    hash._hash = (x | y);
}

static void geohash_neighbors(geo::geo_radius& output)
{
    auto& hash = output._hash;
    output._neighbors._east = hash;
    output._neighbors._west = hash;
    output._neighbors._north = hash;
    output._neighbors._south = hash;
    output._neighbors._south_east = hash;
    output._neighbors._south_west = hash;
    output._neighbors._north_east = hash;
    output._neighbors._north_west = hash;

    geohash_move_x(output._neighbors._east, 1);
    geohash_move_y(output._neighbors._east, 0);

    geohash_move_x(output._neighbors._west, -1);
    geohash_move_y(output._neighbors._west, 0);

    geohash_move_x(output._neighbors._south, 0);
    geohash_move_y(output._neighbors._south, -1);

    geohash_move_x(output._neighbors._north, 0);
    geohash_move_y(output._neighbors._north, 1);

    geohash_move_x(output._neighbors._north_west, -1);
    geohash_move_y(output._neighbors._north_west, 1);

    geohash_move_x(output._neighbors._north_east, 1);
    geohash_move_y(output._neighbors._north_east, 1);

    geohash_move_x(output._neighbors._south_east, 1);
    geohash_move_y(output._neighbors._south_east, -1);

    geohash_move_x(output._neighbors._south_west, -1);
    geohash_move_y(output._neighbors._south_west, -1);
}

static int fetch_points_in_box(geo::geo_hash& h, double longitude, double latitude, double radius, geo::fetch_point& f, geo::points_type& points)
{
    uint64_t min = 0, max = 0;
    auto align_hash = [] (const geo::geo_hash& h) -> uint64_t {
        uint64_t hash = h._hash;
        hash <<= (52 - h._step * 2);
        return hash;
    };
    min = align_hash(h);
    h._hash++;
    max = align_hash(h);
    return f(min, max, longitude, latitude, radius, points);
}

bool geo::fetch_points_from_location(double longitude, double latitude, double radius, fetch_point&& f, points_type& points)
{
    geo_radius output;

    double bounds[4];
    if (geohash_bounding_box(longitude, latitude, radius, bounds) == false) {
        return false;
    }
    double min_lon = bounds[0], max_lon = bounds[2], min_lat = bounds[1], max_lat = bounds[3];

    // 1. step
    output._hash._step = geohash_estimate_steps_by_radius(radius, latitude);

    // 2. hash
    bounds[0] = GEO_LONG_MIN, bounds[1] = GEO_LONG_MAX, bounds[2] = GEO_LAT_MIN, bounds[3] = GEO_LAT_MAX;
    if (geohash_encode_internal(bounds, longitude, latitude, output._hash._step, output._hash._hash) == false) {
        return false;
    }

    // 3. neighbors
    geohash_neighbors(output);


    // 4. area
    if (geohash_decode_internal(bounds, output) == false) {

        return false;
    }

    if (output._area._latitude_range._min < min_lat) {
        output._neighbors._south._hash =  output._neighbors._south_west._hash = output._neighbors._south_east._hash = 0;
        output._neighbors._south._step =  output._neighbors._south_west._step = output._neighbors._south_east._step = 0;
    }
    if (output._area._latitude_range._max > max_lat) {
        output._neighbors._north._hash = output._neighbors._north_east._hash = output._neighbors._north_west._hash = 0;
        output._neighbors._north._step = output._neighbors._north_east._step = output._neighbors._north_west._step = 0;
    }
    if (output._area._longitude_range._min < min_lon) {
        output._neighbors._west._hash = output._neighbors._south_west._hash = output._neighbors._north_west._hash = 0;
        output._neighbors._west._step = output._neighbors._south_west._step = output._neighbors._north_west._step = 0;
    }
    if (output._area._longitude_range._max > max_lon) {
        output._neighbors._east._hash = output._neighbors._south_east._hash = output._neighbors._north_east._hash = 0;
        output._neighbors._east._step = output._neighbors._south_east._step = output._neighbors._north_east._step = 0;
    }

    geo_hash gh[9] = { output._hash,
                    output._neighbors._north,
                    output._neighbors._east,
                    output._neighbors._west,
                    output._neighbors._south,
                    output._neighbors._north_east,
                    output._neighbors._south_east,
                    output._neighbors._north_west,
                    output._neighbors._south_west
    };
    size_t count = 0;
    int last_processed = 0;
    for (int i = 0; i < 9; ++i) {
        auto& h = gh[i];
        if (h._hash == 0 && h._step == 0) {
            continue;
        }
        if (last_processed && gh[i]._hash == gh[last_processed]._hash && gh[i]._step == gh[last_processed]._step) {
            continue;
        }
        count += fetch_points_in_box(gh[i], longitude, latitude, radius, f, points);
        last_processed = i;
    }
    return true;
}

bool geo::to_meters(double& n, int flags)
{
    if (flags & GEO_UNIT_M) {
    }
    else if (flags & GEO_UNIT_KM) {
        n *= 1000;
    }
    else if (flags & GEO_UNIT_MI) {
        n *= 0.3048;
    }
    else if (flags & GEO_UNIT_FT) {
        n *= 1609.34;
    }
    else {
        return false;
    }
    return true;
}

bool geo::from_meters(double& n, int flags)
{
    if (flags & GEO_UNIT_M) {
    }
    else if (flags & GEO_UNIT_KM) {
        n /= 1000;
    }
    else if (flags & GEO_UNIT_MI) {
        n /= 0.3048;
    }
    else if (flags & GEO_UNIT_FT) {
        n /= 1609.34;
    }
    else {
        return false;
    }
    return true;
}
}
