# Configurable HTTP/2 write buffer

This fork adds `http2.Server.WriteBufferSize` to configure the connection-level
`bufio.Writer` used by the HTTP/2 server.

## Changes

- `v0.0.0-watchbuf64k.1`: increases the default connection output buffer from
  4 KiB to 64 KiB.
- `v0.0.0-watchbuf64k.2`: exposes `Server.WriteBufferSize` so each server can
  choose its output buffer size. Zero and negative values default to 64 KiB.
- Common power-of-two sizes from 4 KiB through 1 MiB use separate shared buffer
  pools. Other positive sizes are allocated exactly and are not pooled.
- Buffers are allocated lazily and released on flush. Explicit flush logic,
  per-stream buffers, and HTTP/2 flow-control windows are unchanged. The point
  at which a full buffer automatically writes to the connection depends on its size.

The fork is based on `golang.org/x/net` revision `a5a99cb37ef4`.
It is a targeted change to that historical version, not an update to current upstream.

## Dependency

Add or update this replacement in your application's `go.mod`:

```go
replace golang.org/x/net => github.com/zht255/net-http2 v0.0.0-watchbuf64k.3
```

Keep imports as `golang.org/x/net/http2`. Synchronize `go.sum` and, if your project
uses vendoring, regenerate `vendor` through your project's dependency workflow.

## Usage

Set the field before the server starts accepting connections:

```go
package example

import (
    "net/http"

    "golang.org/x/net/http2"
)

func ConfigureHTTP2(server *http.Server) error {
    options := &http2.Server{
        WriteBufferSize: 64 << 10, // 65536 bytes
        EnableDataFrameAllocationOptimization: true,
        EnableWriteDataPoolCleanup: true,
    }
    return http2.ConfigureServer(server, options)
}
```

If your application already configures an `http2.Server`, set
`options.WriteBufferSize` on that existing configuration and preserve its other
settings. The field also applies when using `Server.ServeConn` directly.
It does not configure the Go standard library's implicit HTTP/2 server.

For comparisons, use `4 << 10`, `16 << 10`, `32 << 10`, or `64 << 10`.
The unit is bytes; zero does not disable buffering. Runtime mutation while
serving connections is unsupported. This setting applies to all connections
served by that HTTP/2 server, not just a particular request or stream.

## Validation and tradeoffs

Tests cover configured sizes, lazy allocation, framed byte preservation,
buffer reuse across sizes, flush errors, and configuration on real server
connections. The HTTP/2 package suite and ten rounds of targeted race tests
passed for version `v0.0.0-watchbuf64k.2`:

```sh
go test ./http2/... -count=1 -timeout=5m
go test -race ./http2 -run '^(TestBufferedWriter|TestServerWriteBufferSize|TestServer_MaxQueuedControlFrames)' -count=10 -timeout=2m
```

A synthetic batch of eight 4096-byte payloads with four-byte length prefixes
produces 32827 bytes after HTTP/2 DATA framing. In the test sink, buffer sizes
of 4/16/32/64 KiB result in 3/3/2/1 underlying writes per batch. These are local
writer call counts, not measured TLS/TCP syscall counts or end-to-end throughput.

Larger buffers can reduce underlying writes but use more memory per active
writer and in the shared pools. Measure throughput, latency, allocations, and
memory under your own workload before selecting a size.

## Per-optimization configuration (version .3)

All fields belong to `http2.Server`. Set them before `ConfigureServer` and before
accepting connections; runtime mutation is unsupported. No application flags
are registered automatically. Applications can map their own startup flags to
these fields. The two new booleans default to false for independent A/B tests
and rollback to version .2 behavior.

### Connection output buffering

Parameter: `WriteBufferSize int`, in bytes; values <= 0 use 65536.
Use `options.WriteBufferSize = 64 << 10` for an initial batched-write comparison;
compare 4, 16, 32, and 64 KiB under the same workload. This controls the connection
`bufio.Writer`, not the TCP send buffer or the stream flow-control window.
Larger buffers trade memory for potentially fewer underlying writes. The
synthetic batch result and its limits are described above.

### Split DATA descriptor allocation

Parameter: `EnableDataFrameAllocationOptimization bool`, default false.
Set `options.EnableDataFrameAllocationOptimization = true` to allocate the two
resulting descriptors together when DATA must be split by frame size or flow
control. It preserves the original payload, frame limits, END_STREAM placement,
and final completion notification. Whole frames and blocked writes are unchanged.

Local Go 1.20 microbenchmarks reduce each split from 2 allocations / 96 bytes
to 1 allocation / 80 bytes. This is an allocation reduction, not a promised
end-to-end throughput improvement. Set false and restart for an A/B baseline.

### Completed write descriptor cleanup

Parameter: `EnableWriteDataPoolCleanup bool`, default false.
Set `options.EnableWriteDataPoolCleanup = true` to clear completed descriptors
before returning them to the shared pool. This releases their payload references;
it does not overwrite payload bytes. Requests still owned by the writer after
an early disconnect are neither cleared nor returned to the pool.

A completed descriptor previously retained a 1 MiB test payload; with cleanup
its payload reference is nil. This does not imply a 1 MiB RSS reduction or faster
network throughput. Set false and restart to disable cleanup. Pools are shared,
so this affects descriptors returned by this server, not every existing entry.

### Validation

```sh
go test ./http2/... -count=1 -timeout=5m
go test -race ./http2 -run 'TestConfigurable' -count=10 -timeout=3m
go test ./http2 -run '^$' -bench '^BenchmarkConfigurableSplit$' -benchmem -count=5
```

Tests cover all four combinations of the two switches, 4/64 KiB output buffers,
concurrent real TLS HTTP/2 responses, repeated splitting, END_STREAM/completion
ownership, and cleanup behavior on completion versus early disconnect.
DATA direct-copy avoidance, queue compaction, and TLS record batching/prefetch
experiments are not included in version .3.
