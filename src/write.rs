use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::{WriteBackpressure, WriteError};

#[derive(Clone, Copy, Debug)]
pub struct WriteHandoffConfig {
    pub max_items: usize,
    pub max_pending_bytes: usize,
}

impl WriteHandoffConfig {
    pub fn new(max_items: usize, max_pending_bytes: usize) -> Self {
        Self {
            max_items,
            max_pending_bytes,
        }
    }
}

pub struct WriteHandoff {
    tx: mpsc::Sender<WriteRequest>,
    budget: Arc<ByteBudget>,
    closed: Arc<AtomicBool>,
}

#[derive(Debug)]
pub struct WriteTicket {
    rx: oneshot::Receiver<WriteCompletion>,
}

#[derive(Debug)]
pub struct WriteCompletion {
    result: Result<(), WriteError>,
}

struct WriteRequest {
    bytes: Bytes,
    completion: oneshot::Sender<WriteCompletion>,
    budget: Arc<ByteBudget>,
}

struct ByteBudget {
    pending: AtomicUsize,
    limit: usize,
}

impl WriteHandoff {
    pub fn spawn<W>(writer: W, config: WriteHandoffConfig) -> Self
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::channel(config.max_items);
        let budget = Arc::new(ByteBudget::new(config.max_pending_bytes));
        let closed = Arc::new(AtomicBool::new(false));
        tokio::spawn(writer_loop(writer, rx, closed.clone()));

        Self { tx, budget, closed }
    }

    pub fn try_write(&self, bytes: Bytes) -> Result<WriteTicket, WriteBackpressure> {
        if self.closed.load(Ordering::Acquire) {
            return Err(WriteBackpressure::closed(bytes));
        }
        self.budget.try_acquire(bytes.len()).map_err(|_| {
            WriteBackpressure::byte_budget_exceeded(
                bytes.clone(),
                self.pending_bytes().saturating_add(bytes.len()),
                self.budget.limit,
            )
        })?;

        let (completion, rx) = oneshot::channel();
        let request = WriteRequest {
            bytes,
            completion,
            budget: self.budget.clone(),
        };
        match self.tx.try_send(request) {
            Ok(()) => Ok(WriteTicket { rx }),
            Err(mpsc::error::TrySendError::Full(request)) => {
                request.budget.release(request.bytes.len());
                Err(WriteBackpressure::queue_full(request.bytes))
            }
            Err(mpsc::error::TrySendError::Closed(request)) => {
                request.budget.release(request.bytes.len());
                self.closed.store(true, Ordering::Release);
                Err(WriteBackpressure::closed(request.bytes))
            }
        }
    }

    pub async fn write(&self, bytes: Bytes) -> Result<WriteTicket, WriteError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(WriteError::Closed);
        }
        self.budget
            .try_acquire(bytes.len())
            .map_err(|_| WriteError::ByteBudgetExceeded {
                attempted: self.pending_bytes().saturating_add(bytes.len()),
                limit: self.budget.limit,
            })?;

        let (completion, rx) = oneshot::channel();
        let request = WriteRequest {
            bytes,
            completion,
            budget: self.budget.clone(),
        };
        if let Err(err) = self.tx.send(request).await {
            let request = err.0;
            request.budget.release(request.bytes.len());
            self.closed.store(true, Ordering::Release);
            return Err(WriteError::Closed);
        }
        Ok(WriteTicket { rx })
    }

    pub fn pending_bytes(&self) -> usize {
        self.budget.pending.load(Ordering::Acquire)
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
}

impl Clone for WriteHandoff {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            budget: self.budget.clone(),
            closed: self.closed.clone(),
        }
    }
}

impl WriteTicket {
    pub async fn wait(self) -> Result<(), WriteError> {
        match self.rx.await {
            Ok(completion) => completion.result,
            Err(_) => Err(WriteError::Closed),
        }
    }
}

impl ByteBudget {
    fn new(limit: usize) -> Self {
        Self {
            pending: AtomicUsize::new(0),
            limit,
        }
    }

    fn try_acquire(&self, bytes: usize) -> Result<(), ()> {
        let mut current = self.pending.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return Err(());
            };
            if next > self.limit {
                return Err(());
            }
            match self.pending.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    fn release(&self, bytes: usize) {
        self.pending.fetch_sub(bytes, Ordering::AcqRel);
    }
}

async fn writer_loop<W>(
    mut writer: W,
    mut rx: mpsc::Receiver<WriteRequest>,
    closed: Arc<AtomicBool>,
) where
    W: AsyncWrite + Unpin,
{
    while let Some(request) = rx.recv().await {
        let len = request.bytes.len();
        let result = writer
            .write_all(&request.bytes)
            .await
            .map_err(WriteError::Io);
        request.budget.release(len);
        let should_close = result.is_err();
        let _ = request.completion.send(WriteCompletion { result });
        if should_close {
            closed.store(true, Ordering::Release);
            break;
        }
    }
    closed.store(true, Ordering::Release);

    while let Ok(request) = rx.try_recv() {
        request.budget.release(request.bytes.len());
        let _ = request.completion.send(WriteCompletion {
            result: Err(WriteError::Closed),
        });
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn writes_owned_bytes_in_order() {
        let (client, mut server) = tokio::io::duplex(64);
        let handoff = WriteHandoff::spawn(client, WriteHandoffConfig::new(4, 64));

        let first = handoff
            .try_write(Bytes::from_static(b"abc"))
            .expect("first handoff");
        let second = handoff
            .try_write(Bytes::from_static(b"def"))
            .expect("second handoff");

        first.wait().await.expect("first write completes");
        second.wait().await.expect("second write completes");

        let mut out = [0_u8; 6];
        server
            .read_exact(&mut out)
            .await
            .expect("read written bytes");
        assert_eq!(&out, b"abcdef");
        assert_eq!(handoff.pending_bytes(), 0);
    }

    #[tokio::test]
    async fn try_write_reports_byte_backpressure_without_losing_bytes() {
        let (client, _server) = tokio::io::duplex(64);
        let handoff = WriteHandoff::spawn(client, WriteHandoffConfig::new(4, 3));

        let err = handoff
            .try_write(Bytes::from_static(b"abcd"))
            .expect_err("over budget");
        assert_eq!(err.into_bytes(), Bytes::from_static(b"abcd"));
        assert_eq!(handoff.pending_bytes(), 0);
    }

    #[tokio::test]
    async fn async_write_reserves_byte_budget() {
        let (client, mut server) = tokio::io::duplex(64);
        let handoff = WriteHandoff::spawn(client, WriteHandoffConfig::new(4, 8));

        let ticket = handoff
            .write(Bytes::from_static(b"hello"))
            .await
            .expect("handoff");
        assert!(handoff.pending_bytes() <= 8);
        ticket.wait().await.expect("completion");

        let mut out = [0_u8; 5];
        server
            .read_exact(&mut out)
            .await
            .expect("read written bytes");
        assert_eq!(&out, b"hello");
    }
}
