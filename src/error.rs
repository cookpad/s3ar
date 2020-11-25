use std::fmt;
use std::error::Error as StdError;

use tokio::{io, task, sync::mpsc};
use rusoto_core::RusotoError;

use super::chan_exec;

#[derive(Debug)]
pub struct StringError(String);
impl fmt::Display for StringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl StdError for StringError {}
impl From<String> for StringError {
    fn from(s: String) -> StringError {
        StringError(s)
    }
}

#[derive(Debug)]
pub struct StaticStrError(&'static str);
impl fmt::Display for StaticStrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl StdError for StaticStrError {}
impl From<&'static str> for StaticStrError {
    fn from(s: &'static str) -> StaticStrError {
        StaticStrError(s)
    }
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Nix(nix::Error),
    ChanExec(chan_exec::Error<()>),
    Rusoto(RusotoError<StringError>),
    JoinError(task::JoinError),
    String(StringError),
    StaticStr(StaticStrError),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.source().unwrap())
    }
}
impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            &Self::Io(ref e) => Some(e),
            &Self::Nix(ref e) => Some(e),
            &Self::ChanExec(ref e) => Some(e),
            &Self::Rusoto(ref e) => Some(e),
            &Self::JoinError(ref e) => Some(e),
            &Self::String(ref e) => Some(e),
            &Self::StaticStr(ref e) => Some(e),
        }
    }
}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<nix::Error> for Error {
    fn from(e: nix::Error) -> Self {
        Self::Nix(e)
    }
}
impl<Q> From<chan_exec::Error<Q>> for Error {
    fn from(e: chan_exec::Error<Q>) -> Self {
        use chan_exec::Error as CEError;
        use mpsc::error::SendError as MpscSendError;
        Self::ChanExec(match e {
            CEError::Send(_) => CEError::Send(MpscSendError(())),
            CEError::Recv(c) => CEError::Recv(c),
        })
    }
}
impl<E> From<RusotoError<E>> for Error
where
    E: fmt::Display,
{
    fn from(e: RusotoError<E>) -> Self {
        Self::Rusoto(match e {
            RusotoError::Service(e) => RusotoError::Service(format!("{}", e).into()),
            RusotoError::HttpDispatch(e) => RusotoError::HttpDispatch(e),
            RusotoError::Credentials(e) => RusotoError::Credentials(e),
            RusotoError::Validation(e) => RusotoError::Validation(e),
            RusotoError::ParseError(e) => RusotoError::ParseError(e),
            RusotoError::Unknown(e) => RusotoError::Unknown(e),
        })
    }
}
impl From<task::JoinError> for Error {
    fn from(e: task::JoinError) -> Self {
        Self::JoinError(e)
    }
}
impl From<StringError> for Error {
    fn from(e: StringError) -> Self {
        Self::String(e)
    }
}
impl From<String> for Error {
    fn from(s: String) -> Self {
        Self::String(StringError(s))
    }
}
impl From<StaticStrError> for Error {
    fn from(e: StaticStrError) -> Self {
        Self::StaticStr(e)
    }
}
impl From<&'static str> for Error {
    fn from(e: &'static str) -> Self {
        Self::StaticStr(StaticStrError(e))
    }
}
