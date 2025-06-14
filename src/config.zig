const std = @import("std");

// The caller owns the resulting hashmap that was allocated with the given allocator
// Slices of strings in the options point to argv
pub fn parseCommandLineArguments(allocator: std.mem.Allocator) !std.StringArrayHashMap([]const u8) {
    var config = std.ArrayList(struct { []const u8, []const u8 }).init(allocator);
    errdefer config.deinit();
    const arg_type = enum { STRING, U16 };
    const available_options: struct { short: []const ?u8, long: []const ?[]const u8, description: []const []const u8, arg_type: []const ?arg_type } = .{
        .short = &[_]?u8{ 'h', null, null, 'p', null, null },
        .long = &[_]?[]const u8{ "help", "dir", "dbfilename", "port", "replicaof", "diewithmaster" },
        .description = &[_][]const u8{ "Display this help and exit", "Directory where dbfilename can be found", "The name of a .rdb file to load on startup", "The port to listen on", "The master instance for this replica (e.g. \"127.0.0.1 6379\")", "If this instance is a slave and its master disconnects, exit out" },
        .arg_type = &[_]?arg_type{ null, .STRING, .STRING, .U16, .STRING, null },
    };

    var args_it = std.process.ArgIterator.init();
    const program_name = args_it.next().?;

    var temp_allocator = std.heap.ArenaAllocator.init(allocator);
    defer temp_allocator.deinit();

    var help_message = std.ArrayList(u8).init(temp_allocator.allocator());
    const help_message_writer = help_message.writer();

    try std.fmt.format(help_message_writer, "Usage: {s} [options..]\n", .{program_name});
    try std.fmt.format(help_message_writer, "Options:\n", .{});
    for (0..available_options.short.len) |i| {
        var arg_column = std.ArrayList(u8).init(temp_allocator.allocator());
        if (available_options.short[i] != null and available_options.long[i] != null) {
            try std.fmt.format(arg_column.writer(), "-{c}, --{s}", .{ available_options.short[i].?, available_options.long[i].? });
        } else if (available_options.short[i] != null) {
            try std.fmt.format(arg_column.writer(), "-{}", .{available_options.short[i].?});
        } else {
            try std.fmt.format(arg_column.writer(), "--{s}", .{available_options.long[i].?});
        }
        if (available_options.arg_type[i]) |arg| {
            switch (arg) {
                .U16 => try std.fmt.format(arg_column.writer(), " <0-65535>", .{}),
                .STRING => try std.fmt.format(arg_column.writer(), " <str>", .{}),
            }
        }
        try std.fmt.format(help_message_writer, "\t{s: <20}\t{s}\n", .{ arg_column.items, available_options.description[i] });
    }
    const stderr = std.io.getStdErr();

    var options = std.StringArrayHashMap([]const u8).init(allocator);
    errdefer options.deinit();

    while (args_it.next()) |arg| {
        var n_dashes: u8 = 0;
        var optname = arg;
        if (arg[n_dashes] != '-') {
            _ = try stderr.write(help_message.items);
            return error.PositionalArgument;
        }
        while (arg[n_dashes] == '-') : (n_dashes += 1) {}
        optname = optname[n_dashes..];
        var optidx: ?usize = null;
        switch (n_dashes) {
            1 => {
                inline for (available_options.short, 0..) |maybe_option, i| {
                    if (maybe_option) |option| {
                        if (optname[0] == option) {
                            optidx = i;
                            break;
                        }
                    }
                }
            },
            2 => {
                inline for (available_options.long, 0..) |maybe_option, i| {
                    if (maybe_option) |option| {
                        if (std.mem.eql(u8, option, optname)) {
                            optidx = i;
                            break;
                        }
                    }
                }
            },
            else => return error.InvalidOption,
        }
        if (optidx == null) {
            _ = try stderr.write(help_message.items);
            return error.InvalidOption;
        }

        switch (optidx.?) {
            0 => {
                _ = try stderr.write(help_message.items);
                return error.Help;
            }, // help
            1 => { // dir
                const next_arg = args_it.next() orelse {
                    _ = try stderr.write(help_message.items);
                    return error.MissingArgument;
                };
                if (next_arg[0] == '-') {
                    _ = try stderr.write(help_message.items);
                    return error.InvalidArgument;
                }
                try options.put(optname, next_arg);
            },
            2 => {
                const next_arg = args_it.next() orelse {
                    _ = try stderr.write(help_message.items);
                    return error.MissingArgument;
                };
                if (next_arg[0] == '-') {
                    _ = try stderr.write(help_message.items);
                    return error.InvalidArgument;
                }
                try options.put(optname, next_arg);
            }, // dbfilename
            3 => {
                const next_arg = args_it.next() orelse {
                    _ = try stderr.write(help_message.items);
                    return error.MissingArgument;
                };
                _ = std.fmt.parseInt(u16, next_arg, 10) catch {
                    _ = try stderr.write(help_message.items);
                    return error.InvalidArgument;
                };

                try options.put("listening-port", next_arg);
            }, // port
            4 => { // replicaof
                const next_arg = args_it.next() orelse {
                    _ = try stderr.write(help_message.items);
                    return error.MissingArgument;
                };
                if (next_arg[0] == '-') {
                    _ = try stderr.write(help_message.items);
                    return error.InvalidArgument;
                }

                try options.put("master", next_arg);
            },
            5 => { // diewithmaster
                try options.put("diewithmater", "true");
            },
            else => unreachable,
        }
    }
    return options;
}
