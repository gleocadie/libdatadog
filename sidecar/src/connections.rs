use std::{
    sync::{
        atomic::{
            AtomicUsize,
            Ordering::{Acquire, Relaxed},
        },
        Arc,
    },
    task::{ready, Poll},
    time::Duration,
};

use hyper::server::accept::Accept;
use pin_project::pin_project;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{UnixListener, UnixStream},
    time::timeout,
};

#[pin_project]
#[derive(Debug)]
pub struct UnixListenerTracked {
    listener: UnixListener,
    connection_tracker: Tracker,
}

impl UnixListenerTracked {
    pub fn watch(&self) -> TrackerWatcher {
        self.connection_tracker.watch()
    }
}

impl Accept for UnixListenerTracked {
    type Conn = UnixStreamTracked;

    type Error = std::io::Error;

    fn poll_accept(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Self::Conn, Self::Error>>> {
        let stream = ready!(self.listener.poll_accept(cx))?.0;
        println!("UnixListenerTracker is polling to accept new connection");
        Poll::Ready(Some(Ok(UnixStreamTracked {
            inner: stream,
            tracker: self.connection_tracker.clone(),
        })))
    }
}

impl From<UnixListener> for UnixListenerTracked {
    fn from(listener: UnixListener) -> Self {
        Self {
            listener,
            connection_tracker: Tracker::default(),
        }
    }
}

#[pin_project]
pub struct UnixStreamTracked {
    #[pin]
    inner: UnixStream,
    tracker: Tracker,
}

impl AsyncWrite for UnixStreamTracked {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        self.project().inner.poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_shutdown(cx)
    }
}

impl AsyncRead for UnixStreamTracked {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.project().inner.poll_read(cx, buf)
    }
}

#[derive(Debug, Default)]
pub struct Tracker {
    count: Arc<AtomicUsize>,
    notifier: Arc<tokio::sync::Notify>,
}

impl Drop for Tracker {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Relaxed);
        self.notifier.notify_waiters();
    }
}

impl Clone for Tracker {
    fn clone(&self) -> Self {
        self.count.fetch_add(1, Relaxed);
        self.notifier.notify_waiters();
        Self {
            count: self.count.clone(),
            notifier: self.notifier.clone(),
        }
    }
}

impl Tracker {
    pub fn watch(&self) -> TrackerWatcher {
        TrackerWatcher {
            count: self.count.clone(),
            notifier: self.notifier.clone(),
        }
    }
}

pub struct TrackerWatcher {
    count: Arc<AtomicUsize>,
    notifier: Arc<tokio::sync::Notify>,
}

impl TrackerWatcher {
    pub async fn wait_for_no_instances(&self, min_duration_without_instances: Duration) {
        println!("wait_for_no_instances in TrackerWatcher");
        let mut prev_count = self.count.load(Relaxed);
        let mut prev_time = tokio::time::Instant::now();
        loop {
            if timeout(min_duration_without_instances, self.notifier.notified())
                .await
                .is_err()
                && prev_count == 0
            {
                println!("TrackerWatcher has not been notified within 1 second and count == 0. Returning");
                return;
            }

            let count = self.count.load(Acquire);
            if prev_count == count
                && count == 0
                && prev_time.elapsed() >= min_duration_without_instances
            {
                println!("TrackerWatcher prev_count == count AND count == 0 AND prev_time.elapsed() > 1 second. Returning.");
                return;
            }

            prev_count = count;
            prev_time = tokio::time::Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_a() {}
}
