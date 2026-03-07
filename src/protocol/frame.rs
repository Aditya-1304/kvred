use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
  Simple(String),
  Error(String),
  Integer(i64),
  Bulk(Bytes),
  Null,
  Array(Vec<Frame>),
}