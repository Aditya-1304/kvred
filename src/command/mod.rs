use bytes::Bytes;

pub mod parse;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
  Ping(Option<Bytes>),
  Get { key: Bytes },
  Set { key: Bytes, value: Bytes},
  Del { keys: Vec<Bytes> },
  Exists { keys: Vec<Bytes>}
}