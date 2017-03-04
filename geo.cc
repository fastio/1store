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

bool geo::encode_to_geohash(const double& longtitude, const double& latitude, double& geohash)
{
    if (longtitude > GEO_LONG_MAX || longtitude < GEO_LONG_MIN || latitude > GEO_LAT_MAX || latitude < GEO_LAT_MIN) {
        return false;
    }

    uint64_t bits = 0;
    double lat_offset = (latitude - GEO_LAT_MIN) / (GEO_LAT_SCALE);
    double long_offset = (longtitude - GEO_LONG_MIN) / (GEO_LONG_SCALE);

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
    double longtitude = 0, latitude = 0;
    if (decode_from_geohash(score, longtitude, latitude) == false) {
        return false;
    }
    if (longtitude > GEO_LONG_MAX || longtitude < GEO_LONG_MIN || latitude > GEO_LAT_MAX_STD || latitude < GEO_LAT_MIN_STD) {
        return false;
    }
    uint64_t bits = 0;
    double lat_offset =
        (latitude - GEO_LAT_MIN_STD) / (GEO_LAT_STD_SCALE);
    double long_offset =
        (longtitude - GEO_LONG_MIN) / (GEO_LONG_SCALE);

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
    geohashstr = std::move(sstring(b, 12));
    return true;
}

bool geo::decode_from_geohash(const double& geohash, double& longtitude, double& latitude)
{
    uint64_t hash_sep = deinterleave64((uint64_t)geohash); /* hash = [LAT][LONG] */
    uint32_t ilato = hash_sep;
    uint32_t ilono = hash_sep >> 32;
    double latitude_min =  GEO_LAT_MIN + (ilato * 1.0 / (1ull << GEO_HASH_STEP_MAX)) * GEO_LAT_SCALE;
    double latitude_max =  GEO_LAT_MIN + ((ilato + 1) * 1.0 / (1ull << GEO_HASH_STEP_MAX)) * GEO_LAT_SCALE;
    double longtitude_min = GEO_LONG_MIN + (ilono * 1.0 / (1ull << GEO_HASH_STEP_MAX)) * GEO_LONG_SCALE;
    double longtitude_max = GEO_LONG_MIN + ((ilono + 1) * 1.0 / (1ull << GEO_HASH_STEP_MAX)) * GEO_LONG_SCALE;
    longtitude = (longtitude_min + longtitude_max) / 2.0;
    latitude = (latitude_min + latitude_max) / 2.0;
    return true;
}
}
