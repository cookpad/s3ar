use std::error::Error as StdError;
use std::fmt;

use futures::channel::oneshot;
use futures::prelude::*;
use mpsc::error::SendError as MpscSendError;
use tokio::sync::mpsc;

pub type Task<S> = future::BoxFuture<'static, S>;
type TaskWithCb<S> = (Task<S>, oneshot::Sender<S>);
type Sender<S> = mpsc::Sender<TaskWithCb<S>>;

#[derive(Debug)]
pub enum Error<Q> {
    Send(MpscSendError<Q>),
    Recv(oneshot::Canceled),
}
impl<Q> fmt::Display for Error<Q>
where
    Q: fmt::Debug + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.source().unwrap())
    }
}
impl<Q> StdError for Error<Q>
where
    Q: fmt::Debug + 'static,
{
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Send(ref e) => Some(e),
            Self::Recv(ref e) => Some(e),
        }
    }
}
impl<Q> From<mpsc::error::SendError<Q>> for Error<Q> {
    fn from(e: mpsc::error::SendError<Q>) -> Self {
        Self::Send(e)
    }
}
impl<Q> From<oneshot::Canceled> for Error<Q> {
    fn from(e: oneshot::Canceled) -> Self {
        Self::Recv(e)
    }
}

#[derive(Debug)]
pub struct ChanExec<S> {
    sender: Sender<S>,
}

impl<S> Clone for ChanExec<S> {
    fn clone(&self) -> Self {
        ChanExec {
            sender: self.sender.clone(),
        }
    }
}

impl<S> ChanExec<S> {
    pub async fn execute(&mut self, task: Task<S>) -> Result<S, Error<Task<S>>>
    where
        S: 'static,
    {
        let (tx, rx) = oneshot::channel();
        let paired_req = (task, tx);
        self.sender
            .send(paired_req)
            .await
            .map_err(|MpscSendError((req, _tx))| MpscSendError(req))?;
        Ok(rx.await?)
    }
}

pub trait Work<Q, S> {
    type Fut: std::future::Future<Output = S>;
    fn work(&mut self, req: Q) -> Self::Fut;
}

impl<Q, S, F, Fut> Work<Q, S> for F
where
    F: FnMut(Q) -> Fut,
    Fut: std::future::Future<Output = S>,
{
    type Fut = Fut;
    fn work(&mut self, req: Q) -> Self::Fut {
        self(req)
    }
}

pub fn create<S>(queue_size: usize) -> (
    ChanExec<S>,
    impl Stream<Item = impl Future<Output = Result<(), S>>>,
) {
    let (tx, rx) = mpsc::channel::<TaskWithCb<S>>(queue_size);
    let chan_exec = ChanExec { sender: tx };
    let tasks = rx.map(|(task, cb)| async move {
        cb.send(task.await)
    });
    (chan_exec, tasks)
}
