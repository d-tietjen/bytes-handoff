# bytes-handoff

`bytes-handoff` is a small Rust crate for moving owned byte buffers across
async I/O boundaries.

It does not replace `AsyncRead` or `AsyncWrite`. It layers on top of them so
protocol code can:

- read bytes as soon as they arrive
- peek at incomplete input without committing
- preserve unconsumed tails across parser or mode boundaries
- split complete prefixes into `Bytes` for cheap cross-task handoff
- submit owned `Bytes` writes to an async writer without borrowing memory until
  the socket finishes
- bound queued writes by item count and byte count

The zero-copy claim here is intentionally scoped: `BytesMut`/`Bytes` avoid
extra application-level copies after socket ingress. They do not make TCP
kernel-to-userspace reads zero-copy.

## Read Handoff

```rust
use bytes_handoff::HandoffBuffer;

# async fn example<R>(reader: &mut R) -> Result<(), Box<dyn std::error::Error>>
# where
#     R: tokio::io::AsyncRead + Unpin,
# {
let mut buffer = HandoffBuffer::new(64 * 1024);

buffer.read_available(reader).await?;

if let Some(newline) = buffer.peek().iter().position(|b| *b == b'\n') {
    let line = buffer.split_prefix(newline + 1)?;
    send_to_worker(line);
}
# Ok(())
# }
#
# fn send_to_worker(_: bytes::Bytes) {}
```

## Write Handoff

```rust
use bytes::Bytes;
use bytes_handoff::{WriteHandoff, WriteHandoffConfig};

# fn example<W>(writer: W) -> Result<(), Box<dyn std::error::Error>>
# where
#     W: tokio::io::AsyncWrite + Unpin + Send + 'static,
# {
let handoff = WriteHandoff::spawn(writer, WriteHandoffConfig::new(1024, 8 * 1024 * 1024));

let ticket = handoff.try_write(Bytes::from_static(b"owned bytes"))?;

// The producer can continue immediately. Await the ticket only if it needs
// completion/failure confirmation.
# let _ = ticket;
# Ok(())
# }
```
