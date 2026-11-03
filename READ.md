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
replace golang.org/x/net => github.com/zht255/net-http2 v0.0.0-watchbuf64k.2
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
