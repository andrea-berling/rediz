# Rediz

This is my take on the ["Build your own Redis" Challenge](https://app.codecrafters.io/courses/redis/overview) by [CodeCrafters](https://codecrafters.io). It's a Redis re-implementation written in Zig supporting a subset of the features and commands of Redis. It's mostly for didactic purposes, and is therefore not even close to feature complete or fast (it is memory leaks free though :)). This is my first Zig project, and it was a very nice way to learn the language as well as what goes into creating a key-value store like Redis (e.g. FSMs, asynchronous event loop, RDB files, etc.) . If you want to give it a spin, you can find some instructions below.

> [!NOTE]
> Due to the OS-level APIs used in this project, it is only compatible with Linux 5.1+

# Features

Rediz supports the following features:
 - `PING` and `ECHO` commands
 - Concurrent connections via an `io_uring`-based, single-threaded, asynchronous event loop
 - `GET` and `SET` commands
 - Data expiration for `SET` (via the `EX`, `PX`, `EXAT`, and `PXAT` options)
 - Parsing an `RDB` file to initialize the datastore
 - Master slave replication
 - The `WAIT` command
 - Streams (`XADD`, `XRANGE`, `XREAD`) and blocking reads for streams
 - The `INCR` command
 - Transactions (via the `MULTI`, `EXEC`, and `DISCARD` commands)

# Getting started

To get started, build the tool with the following command:

> [!NOTE]
> This project was set up to work with and tested with Zig version 0.14. The instructions below assume you are running that version.

```bash
zig build
```

You will then find the executable under `zig-out/bin`

```bash
$ ./rediz --help
Usage: ./rediz [options..]
Options:
        -h, --help              Display this help and exit
        --dir <str>             Directory where dbfilename can be found
        --dbfilename <str>      The name of a .rdb file to load on startup
        -p, --port <0-65535>    The port to listen on
        --replicaof <str>       The master instance for this replica (e.g. "127.0.0.1 6379")
        --diewithmaster         If this instance is a slave and its master disconnects, exit out
```
Here is a list of things you can do with Rediz:

## PING

Terminal 1:
```bash
$ ./rediz
```

Terminal 2:
```bash
$ redis-cli PING
PONG
$ echo 'PING\nPING\nPING' | redis-cli
PONG
PONG
PONG
```

## ECHO

Terminal 2:
```bash
$ redis-cli ECHO "hello world"
hello world
```

## SET and GET

Terminal 2:
```bash
$ redis-cli GET foo
(nil)
$ redis-cli SET foo bar
OK
$ redis-cli GET foo
"bar"
$ redis-cli SET bar baz EX 5
OK
$ i=5
$ while [[ "$i" -gt 0 ]]; do
>  redis-cli GET bar
>  sleep 1
>  i=$(($i-1))
> done
"baz"
"baz"
"baz"
"baz"
(nil)
```

## Initializing from an RDB file

Terminal 1:
```bash
$ ./rediz --dbfilename dump.rdb --dir "$PWD"
```

Terminal 2:
```bash
$ redis-cli KEYS \*
1) "bar"
2) "foo"
$ redis-cli GET foo
"bar"
$ redis-cli GET bar
"baz"
```

If you look closely at the provided dump, there is also an expired key `baz`, which was not loaded by Rediz at startup.

## Master-slave replication

Terminal 1:
```bash
$ ./rediz
```

Terminal 2:
```bash
$ ./rediz -p 6380 --replicaof '127.0.0.1 6379'
```

Terminal 3:
```bash
$ redis-cli INFO REPLICATION
role:master
master_replid:94cfadf11b45df5af9ed8f3a51c73c65f6b6241c
master_repl_offset:0
$ redis-cli -p 6380 INFO REPLICATION
role:slave
master_replid:94cfadf11b45df5af9ed8f3a51c73c65f6b6241c
master_repl_offset:0
$ redis-cli SET foo bar
OK
$ redis-cli -p 6380 GET foo
"bar"
$ redis-cli WAIT 1 1000
(integer 1)
$ time redis-cli WAIT 2 1000
(integer) 1
redis-cli WAIT 2 1000  0.00s user 0.00s system 0% cpu 1.006 total
$ redis-cli -p 6380 INFO REPLICATION
role:slave
master_replid:94cfadf11b45df5af9ed8f3a51c73c65f6b6241c
master_repl_offset:105
```

## Streams

Terminal 1:
```bash
$ ./rediz
```

Terminal 2:
```bash
$ redis-cli
127.0.0.1:6379> XADD stream_key 0-1 foo bar
"0-1"
127.0.0.1:6379> XADD stream_key 0-* foo bar
"0-2"
127.0.0.1:6379> XADD stream_key * foo bar
"1750065102438-0"
127.0.0.1:6379> XRANGE stream_key - +
1) 1) "0-1"
   2) 1) "foo"
      2) "bar"
2) 1) "0-2"
   2) 1) "foo"
      2) "bar"
3) 1) "1750065102438-0"
   2) 1) "foo"
      2) "bar"
127.0.0.1:6379> XRANGE stream_key 0-2 +
1) 1) "0-2"
   2) 1) "foo"
      2) "bar"
2) 1) "1750065102438-0"
   2) 1) "foo"
      2) "bar"
127.0.0.1:6379> XRANGE stream_key - 0-2
1) 1) "0-1"
   2) 1) "foo"
      2) "bar"
2) 1) "0-2"
   2) 1) "foo"
      2) "bar"
127.0.0.1:6379> XADD other_stream_key 0-2 foo bar
"0-2"
127.0.0.1:6379> XREAD streams stream_key other_stream_key 0-0 0-1
1) 1) "stream_key"
   2) 1) 1) "0-1"
         2) 1) "foo"
            2) "bar"
      2) 1) "0-2"
         2) 1) "foo"
            2) "bar"
      3) 1) "1750065102438-0"
         2) 1) "foo"
            2) "bar"
2) 1) "other_stream_key"
   2) 1) 1) "0-2"
         2) 1) "foo"
            2) "bar"
127.0.0.1:6379> XREAD block 2000 streams stream_key 1750065102438-0
<waiting...>
1) 1) "some_key"
   2) 1) 1) "1526985054069-0"
         2) 1) "foo"
            2) "bar"
```

Terminal 3:
```bash
$ redis-cli XREAD block 2000 streams some_key 1526985054069-0
"1526985054069-0"
```

Terminal 2:
```bash
$ time redis-cli XREAD block 2000 streams some_key 1526985054069-0
(nil)
redis-cli XREAD block 2000 streams some_key 1526985054069-0  0.00s user 0.00s system 0% cpu 2.007 total
```

## Transactions

Terminal 2:
```bash
$ redis-cli
127.0.0.1:6379> MULTI
OK
127.0.0.1:6379(TX)> SET foo 41
QUEUED
127.0.0.1:6379(TX)> INCR foo
QUEUED
127.0.0.1:6379(TX)> EXEC
1) OK
2) (integer) 42
127.0.0.1:6379> EXEC
(error) ERR EXEC without MULTI
127.0.0.1:6379> MULTI
OK
127.0.0.1:6379(TX)> EXEC
(empty array)
127.0.0.1:6379> MULTI
OK
127.0.0.1:6379(TX)> set foo 41
QUEUED
127.0.0.1:6379(TX)> DISCARD
OK
127.0.0.1:6379> DISCARD
(error) ERR DISCARD without MULTI
127.0.0.1:6379> MULTI
OK
127.0.0.1:6379(TX)> SET foo xyz
QUEUED
127.0.0.1:6379(TX)> INCR foo
QUEUED
127.0.0.1:6379(TX)> SET bar 7
QUEUED
127.0.0.1:6379(TX)> EXEC
1) OK
2) (error) ERR value is not an integer or out of range
3) OK
127.0.0.1:6379>

```

## Logging level

You can increase the logging level by changing it in the `src/main.zig` file. For example:

```zig
 pub const std_options: std.Options = .{
     // Define logFn to override the std implementation
-    .log_level = .info,
+    .log_level = .debug,
     .logFn = logger,
 };
```

This will increase the verbosity of the logs of the Rediz instance, useful for troubleshooting.
