use bytes::{Buf, BytesMut};
use std::str;
use crate::protocol::frame::Frame;

#[derive(Debug)]
pub enum DecodeError {
  InvalidPrefix(u8),
  InvalidSimpleString,
}

pub fn decode(buffer: &mut BytesMut) -> Result<Option<Frame>, DecodeError> {
  if buffer.is_empty() {
    return Ok(None);
  }

  match buffer[0] {
      b'+' => decode_simple_string(buffer),
      other => Err(DecodeError::InvalidPrefix(other)),
  }
}

pub fn find_crlf(buffer: &[u8], start: usize) -> Option<usize> {
  buffer[start..]
    .windows(2)
    .position(|w| w == b"\r\n")
    .map(|pos| start + pos)
}

pub fn decode_simple_string(buffer: &mut BytesMut) -> Result<Option<Frame>, DecodeError> {
  let end = match find_crlf(buffer, 1) {
      Some(end) => end,
      None => return Ok(None)
  };

  let body = &buffer[1..end];

  if body.iter().any(|b| *b == b'\r' || *b == b'\n') {
    return Err(DecodeError::InvalidSimpleString);
  }

  let value = str::from_utf8(body)
    .map_err(|_| DecodeError::InvalidSimpleString)?
    .to_owned();

  buffer.advance(end + 2); 
  Ok(Some(Frame::Simple(value)))
}

#[cfg(test)]
mod test {
  use super::*;
  
  #[test]
  fn decodes_ok() {
    let mut buffer = BytesMut::from(&b"+OK\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();
    assert_eq!(frame, Some(Frame::Simple("OK".to_owned())));
    assert!(buffer.is_empty());
  }

  #[test]
  fn returns_none_on_partial() {
    let mut buffer = BytesMut::from(&b"+OK\r"[..]);
    let frame = decode(&mut buffer).unwrap();
    assert_eq!(frame, None);
    assert_eq!(&buffer[..], b"+OK\r");
  }

  #[test]
  fn leaves_remaining_bytes() {
    let mut buffer = BytesMut::from(&b"+OK\r\n+PONG\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();
    assert_eq!(frame, Some(Frame::Simple("OK".to_owned())));
    assert_eq!(&buffer[..], b"+PONG\r\n")
  }

}