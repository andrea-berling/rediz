const std = @import("std");

// https://github.com/codecrafters-io/redis-geocoding-algorithm

pub const MIN_LATITUDE_DEG: f64 = -85.05112878;
pub const MAX_LATITUDE_DEG: f64 = 85.05112878;
pub const MIN_LONGITUDE_DEG: f64 = -180;
pub const MAX_LONGITUDE_DEG: f64 = 180;

pub const LATITUDE_RANGE_DEG: f64 = MAX_LATITUDE_DEG - MIN_LATITUDE_DEG;
pub const LONGITUDE_RANGE_DEG: f64 = MAX_LONGITUDE_DEG - MIN_LONGITUDE_DEG;
pub const N_DIVISIONS: u8 = 26;
pub const NORMAL_RANGE: f64 = 1 << N_DIVISIONS;

pub const EARTH_RADIUS_METERS: f64 = 6372797.560856;

pub const HORIZONTAL_LENGTH: f64 = EARTH_RADIUS_METERS * std.math.degreesToRadians(
    MAX_LONGITUDE_DEG - MIN_LONGITUDE_DEG,
);
pub const VERTICAL_LENGTH: f64 = EARTH_RADIUS_METERS * std.math.degreesToRadians(
    MAX_LATITUDE_DEG - MIN_LATITUDE_DEG,
);

pub const CELL_LENGTHS_METERS: [N_DIVISIONS]f64 = cellLengths();
pub const CELL_HEIGHTS_METERS: [N_DIVISIONS]f64 = cellHeights();

fn cellLengths() [N_DIVISIONS]f64 {
    comptime var ret: [N_DIVISIONS]f64 = undefined;
    comptime var horizontal: f64 = HORIZONTAL_LENGTH;
    for (0..N_DIVISIONS) |i| {
        horizontal /= 2;
        ret[i] = horizontal;
    }

    return ret;
}

fn cellHeights() [N_DIVISIONS]f64 {
    comptime var ret: [N_DIVISIONS]f64 = undefined;
    comptime var vertical: f64 = VERTICAL_LENGTH;
    for (0..N_DIVISIONS) |i| {
        vertical /= 2;
        ret[i] = vertical;
    }

    return ret;
}

pub const Point = struct {
    latitude: f64,
    longitude: f64,
};

pub fn coordinatesToScore(latitude: f64, longitude: f64) !f64 {
    const latitude_out_of_range = latitude < MIN_LATITUDE_DEG or latitude > MAX_LATITUDE_DEG;
    const longitude_out_of_range = longitude < MIN_LONGITUDE_DEG or longitude > MAX_LONGITUDE_DEG;

    if (latitude_out_of_range and longitude_out_of_range) {
        return error.InvalidLonLat;
    } else if (latitude_out_of_range) {
        return error.InvalidLatitude;
    } else if (longitude_out_of_range) {
        return error.InvalidLongitude;
    }

    // normal_range is 26 bits long, hence these two values will always fit in a u32
    const normalized_latitude: u64 = @intFromFloat(NORMAL_RANGE * (latitude - MIN_LATITUDE_DEG) / LATITUDE_RANGE_DEG);
    const normalized_longitude: u64 = @intFromFloat(NORMAL_RANGE * (longitude - MIN_LONGITUDE_DEG) / LONGITUDE_RANGE_DEG);

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
    const grid_latitude_min = MIN_LATITUDE_DEG + LATITUDE_RANGE_DEG * (grid_lat / NORMAL_RANGE);
    const grid_latitude_max = MIN_LATITUDE_DEG + LATITUDE_RANGE_DEG * ((grid_lat + 1) / NORMAL_RANGE);
    const grid_longitude_min = MIN_LONGITUDE_DEG + LONGITUDE_RANGE_DEG * (grid_lon / NORMAL_RANGE);
    const grid_longitude_max = MIN_LONGITUDE_DEG + LONGITUDE_RANGE_DEG * ((grid_lon + 1) / NORMAL_RANGE);

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

pub fn circleBoundingBox(radius_meters: f64) struct { length: f64, height: f64, n_divisions: usize } {
    var a: usize = 0;
    var b: usize = N_DIVISIONS - 1;

    while (a < b) {
        const m = (a + b) / 2;
        const fits_length_wise = CELL_LENGTHS_METERS[m] >= radius_meters * 2;
        const fits_height_wise = CELL_HEIGHTS_METERS[m] >= radius_meters * 2;
        // Tie breaker
        if (b - a == 1) {
            const fits_length_wise_b = CELL_LENGTHS_METERS[b] >= radius_meters * 2;
            const fits_height_wise_b = CELL_HEIGHTS_METERS[b] >= radius_meters * 2;
            if (fits_length_wise_b and fits_height_wise_b) {
                a = b;
                break;
            }

            break;
        }
        if (fits_length_wise and fits_height_wise) {
            a = m;
        } else {
            b = m;
        }
    }

    return .{
        .length = CELL_LENGTHS_METERS[a],
        .height = CELL_HEIGHTS_METERS[a],
        .n_divisions = a + 1,
    };
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

test "cell sizes" {
    for (0..N_DIVISIONS) |i| {
        std.debug.print("Cell len: {d}, cell height: {d}\n", .{ CELL_LENGTHS_METERS[i], CELL_HEIGHTS_METERS[i] });
    }
}
