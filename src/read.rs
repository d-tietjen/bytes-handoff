use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::BufferError;

#[derive(Clone, Copy, Debug)]
pub struct HandoffBufferConfig {
    pub max_len: usize,
    pub read_reserve: usize,
}

impl HandoffBufferConfig {
    pub fn new(max_len: usize) -> Self {
        Self {
            max_len,
            read_reserve: 16 * 1024,
        }
    }

    pub fn with_read_reserve(mut self, read_reserve: usize) -> Self {
        self.read_reserve = read_reserve;
        self
    }
}

#[derive(Debug)]
pub struct HandoffBuffer {
    buf: BytesMut,
    config: HandoffBufferConfig,
}

impl HandoffBuffer {
    pub fn new(max_len: usize) -> Self {
        Self::with_config(HandoffBufferConfig::new(max_len))
    }

    pub fn with_config(config: HandoffBufferConfig) -> Self {
        Self {
            buf: BytesMut::new(),
            config,
        }
    }

    pub fn from_tail(tail: BytesMut, config: HandoffBufferConfig) -> Result<Self, BufferError> {
        if tail.len() > config.max_len {
            return Err(BufferError::LimitExceeded {
                attempted: tail.len(),
                limit: config.max_len,
            });
        }
        Ok(Self { buf: tail, config })
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.buf.capacity()
    }

    pub fn peek(&self) -> &[u8] {
        &self.buf
    }

    pub fn reserve_read_capacity(&mut self, additional: usize) -> Result<(), BufferError> {
        self.check_limit(additional)?;
        self.buf.reserve(additional);
        Ok(())
    }

    pub async fn read_available<R>(&mut self, reader: &mut R) -> Result<usize, BufferError>
    where
        R: AsyncRead + Unpin,
    {
        let reserve = self.remaining_capacity().min(self.config.read_reserve);
        if reserve == 0 {
            return Err(BufferError::LimitExceeded {
                attempted: self.buf.len() + 1,
                limit: self.config.max_len,
            });
        }
        self.buf.reserve(reserve);
        let mut capped_reader = reader.take(reserve as u64);
        let read = capped_reader.read_buf(&mut self.buf).await?;
        if self.buf.len() > self.config.max_len {
            return Err(BufferError::LimitExceeded {
                attempted: self.buf.len(),
                limit: self.config.max_len,
            });
        }
        Ok(read)
    }

    pub fn split_prefix(&mut self, n: usize) -> Result<Bytes, BufferError> {
        if n > self.buf.len() {
            return Err(BufferError::SplitOutOfBounds {
                requested: n,
                available: self.buf.len(),
            });
        }
        Ok(self.buf.split_to(n).freeze())
    }

    pub fn freeze_all(&mut self) -> Bytes {
        self.buf.split().freeze()
    }

    pub fn take_tail(&mut self) -> BytesMut {
        self.buf.split()
    }

    pub fn advance(&mut self, cnt: usize) -> Result<(), BufferError> {
        if cnt > self.buf.len() {
            return Err(BufferError::SplitOutOfBounds {
                requested: cnt,
                available: self.buf.len(),
            });
        }
        self.buf.advance(cnt);
        Ok(())
    }

    fn remaining_capacity(&self) -> usize {
        self.config.max_len.saturating_sub(self.buf.len())
    }

    fn check_limit(&self, additional: usize) -> Result<(), BufferError> {
        let attempted = self.buf.len().saturating_add(additional);
        if attempted > self.config.max_len {
            return Err(BufferError::LimitExceeded {
                attempted,
                limit: self.config.max_len,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn reads_incrementally_and_preserves_tail() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let mut buffer = HandoffBuffer::new(128);

        client
            .write_all(b"hello\npar")
            .await
            .expect("write to duplex");
        assert_eq!(
            buffer
                .read_available(&mut server)
                .await
                .expect("read first chunk"),
            9
        );

        let newline = buffer
            .peek()
            .iter()
            .position(|b| *b == b'\n')
            .expect("newline present");
        let frame = buffer.split_prefix(newline + 1).expect("split frame");
        assert_eq!(frame, Bytes::from_static(b"hello\n"));
        assert_eq!(buffer.peek(), b"par");

        client
            .write_all(b"tial\n")
            .await
            .expect("write second chunk");
        assert_eq!(
            buffer
                .read_available(&mut server)
                .await
                .expect("read second chunk"),
            5
        );
        assert_eq!(buffer.freeze_all(), Bytes::from_static(b"partial\n"));
    }

    #[tokio::test]
    async fn enforces_buffer_limit_before_reading_more() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let mut buffer =
            HandoffBuffer::with_config(HandoffBufferConfig::new(4).with_read_reserve(4));

        client.write_all(b"abcd").await.expect("write within limit");
        assert_eq!(
            buffer
                .read_available(&mut server)
                .await
                .expect("read within limit"),
            4
        );

        let err = buffer
            .read_available(&mut server)
            .await
            .expect_err("buffer is full");
        assert!(matches!(
            err,
            BufferError::LimitExceeded {
                attempted: 5,
                limit: 4
            }
        ));
    }

    #[test]
    fn take_tail_moves_buffered_state() {
        let mut buffer = HandoffBuffer::new(64);
        buffer.buf.extend_from_slice(b"stateful bytes");

        let tail = buffer.take_tail();
        assert!(buffer.is_empty());
        assert_eq!(&tail[..], b"stateful bytes");

        let inherited =
            HandoffBuffer::from_tail(tail, HandoffBufferConfig::new(64)).expect("tail fits");
        assert_eq!(inherited.peek(), b"stateful bytes");
    }

    #[test]
    fn split_prefix_checks_bounds() {
        let mut buffer = HandoffBuffer::new(64);
        buffer.buf.extend_from_slice(b"abc");

        let err = buffer.split_prefix(4).expect_err("prefix too large");
        assert!(matches!(
            err,
            BufferError::SplitOutOfBounds {
                requested: 4,
                available: 3
            }
        ));
    }
}
