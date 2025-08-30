const std = @import("std");

// https://github.com/codecrafters-io/redis-geocoding-algorithm

pub const MIN_LATITUDE: f64 = -85.05112878;
pub const MAX_LATITUDE: f64 = 85.05112878;
pub const MIN_LONGITUDE: f64 = -180;
pub const MAX_LONGITUDE: f64 = 180;

pub const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
pub const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;
pub const NORMAL_RANGE: f64 = 1 << 26;

pub const EARTH_RADIUS_METERS: f64 = 6372797.560856;

pub const Point = struct {
    latitude: f64,
    longitude: f64,
};

pub fn coordinatesToScore(latitude: f64, longitude: f64) !f64 {
    const latitude_out_of_range = latitude < MIN_LATITUDE or latitude > MAX_LATITUDE;
    const longitude_out_of_range = longitude < MIN_LONGITUDE or longitude > MAX_LONGITUDE;

    if (latitude_out_of_range and longitude_out_of_range) {
        return error.InvalidLonLat;
    } else if (latitude_out_of_range) {
        return error.InvalidLatitude;
    } else if (longitude_out_of_range) {
        return error.InvalidLongitude;
    }

    // normal_range is 26 bits long, hence these two values will always fit in a u32
    const normalized_latitude: u64 = @intFromFloat(NORMAL_RANGE * (latitude - MIN_LATITUDE) / LATITUDE_RANGE);
    const normalized_longitude: u64 = @intFromFloat(NORMAL_RANGE * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE);

    // https://github.com/redis/redis/blob/eac48279ad21b8612038953fefa0dcf926773efc/src/geohash.c#L52-L77
    const B = [_]u64{
        0x5555555555555555, 0x3333333333333333,
        0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF,
        0x0000FFFF0000FFFF,
    };
    const S = [_]u64{ 1, 2, 4, 8, 16 };

    var x = normalized_latitude;
    var y = normalized_longitude;

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

    return @floatFromInt(x | (y << 1));
}

pub fn scoreToCoordinates(score: f64) Point {
    const combined_x_y: u64 = @intFromFloat(score);

    var x = combined_x_y & 0x5555555555555555;
    var y = (combined_x_y >> 1) & 0x5555555555555555;

    const B = [_]u64{
        0x3333333333333333,
        0x0F0F0F0F0F0F0F0F,
        0x00FF00FF00FF00FF,
        0x0000FFFF0000FFFF,
        0x00000000FFFFFFFF,
    };
    const S = [_]u64{ 1, 2, 4, 8, 16 };

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

    const grid_lat: f64 = @floatFromInt(x);
    const grid_lon: f64 = @floatFromInt(y);
    const grid_latitude_min = MIN_LATITUDE + LATITUDE_RANGE * (grid_lat / NORMAL_RANGE);
    const grid_latitude_max = MIN_LATITUDE + LATITUDE_RANGE * ((grid_lat + 1) / NORMAL_RANGE);
    const grid_longitude_min = MIN_LONGITUDE + LONGITUDE_RANGE * (grid_lon / NORMAL_RANGE);
    const grid_longitude_max = MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_lon + 1) / NORMAL_RANGE);

    return .{
        .latitude = (grid_latitude_min + grid_latitude_max) / 2,
        .longitude = (grid_longitude_min + grid_longitude_max) / 2,
    };
}

pub fn distance(p: Point, q: Point) f64 {
    // https://en.wikipedia.org/wiki/Haversine_formula
    const delta_phi = std.math.degreesToRadians(q.latitude - p.latitude);
    const delta_lambda = std.math.degreesToRadians(q.longitude - p.longitude);

    const cos_delta_phi = std.math.cos(delta_phi);
    const cos_delta_lambda = std.math.cos(delta_lambda);
    const cos_phi1 = std.math.cos(std.math.degreesToRadians(p.latitude));
    const cos_phi2 = std.math.cos(std.math.degreesToRadians(q.latitude));

    return 2 * EARTH_RADIUS_METERS * std.math.asin(
        std.math.sqrt(
            (1 - cos_delta_phi + cos_phi1 * cos_phi2 * (1 - cos_delta_lambda)) / 2,
        ),
    );
}

const test_data: [12]struct { name: []const u8, latitude: f64, longitude: f64, score: f64 } = .{
    .{ .name = "Bangkok", .latitude = 13.7220, .longitude = 100.5252, .score = 3962257306574459.0 },
    .{ .name = "Beijing", .latitude = 39.9075, .longitude = 116.3972, .score = 4069885364908765.0 },
    .{ .name = "Berlin", .latitude = 52.5244, .longitude = 13.4105, .score = 3673983964876493.0 },
    .{ .name = "Copenhagen", .latitude = 55.6759, .longitude = 12.5655, .score = 3685973395504349.0 },
    .{ .name = "New Delhi", .latitude = 28.6667, .longitude = 77.2167, .score = 3631527070936756.0 },
    .{ .name = "Kathmandu", .latitude = 27.7017, .longitude = 85.3206, .score = 3639507404773204.0 },
    .{ .name = "London", .latitude = 51.5074, .longitude = -0.1278, .score = 2163557714755072.0 },
    .{ .name = "New York", .latitude = 40.7128, .longitude = -74.0060, .score = 1791873974549446.0 },
    .{ .name = "Paris", .latitude = 48.8534, .longitude = 2.3488, .score = 3663832752681684.0 },
    .{ .name = "Sydney", .latitude = -33.8688, .longitude = 151.2093, .score = 3252046221964352.0 },
    .{ .name = "Tokyo", .latitude = 35.6895, .longitude = 139.6917, .score = 4171231230197045.0 },
    .{ .name = "Vienna", .latitude = 48.2064, .longitude = 16.3707, .score = 3673109836391743.0 },
};

test "coordinates to score" {
    for (test_data) |location| {
        try std.testing.expectEqual(
            location.score,
            try coordinatesToScore(location.latitude, location.longitude),
        );
    }
}

test "score to coordinates" {
    for (test_data) |location| {
        const coordinates = scoreToCoordinates(location.score);
        try std.testing.expectApproxEqAbs(
            location.latitude,
            coordinates.latitude,
            0.00001,
        );
        try std.testing.expectApproxEqAbs(
            location.longitude,
            coordinates.longitude,
            0.00001,
        );
    }
}
