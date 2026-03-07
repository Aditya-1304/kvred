use bytes::{Buf, BytesMut};
use std::str;
use crate::protocol::{encode, frame::Frame};

#[derive(Debug)]
pub enum DecodeError {
  InvalidPrefix(u8),
  InvalidSimpleString,
  InvalidInteger,
  InvalidErrorString,
}

pub fn decode(buffer: &mut BytesMut) -> Result<Option<Frame>, DecodeError> {
  if buffer.is_empty() {
    return Ok(None);
  }

  match buffer[0] {
      b'+' => decode_simple_string(buffer),
      b':' => decode_integer(buffer),
      b'-' => decode_error_string(buffer),
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

pub fn decode_error_string(buffer: &mut BytesMut) -> Result<Option<Frame>, DecodeError> {
  let end = match find_crlf(buffer, 1) {
      Some(end) => end,
      None => return Ok(None)
  };

  let body = &buffer[1..end];

  if body.iter().any(|b| *b == b'\r' || *b == b'\n') {
    return Err(DecodeError::InvalidErrorString);
  }

  let value = str::from_utf8(body)
    .map_err(|_| DecodeError::InvalidErrorString)?
    .to_owned();

  buffer.advance(end + 2);
  Ok(Some(Frame::Error(value)))
}

pub fn decode_integer(buffer: &mut BytesMut) -> Result<Option<Frame>, DecodeError> {
  let end = match find_crlf(buffer, 1) {
      Some(end) => end,
      None => return Ok(None)
  };

  let body = &buffer[1..end];

  if body.iter().any(|b| *b == b'\r' || *b == b'\n') {
    return Err(DecodeError::InvalidInteger);
  }

  let value = str::from_utf8(body)
    .map_err(|_| DecodeError::InvalidInteger)?
    .parse::<i64>()
    .map_err(|_| DecodeError::InvalidInteger)?;

  buffer.advance(end + 2);
  Ok(Some(Frame::Integer(value)))
} 


#[cfg(test)]
mod test {
  use super::*;
  
  #[test]
  fn simple_string_decodes_ok() {
    let mut buffer = BytesMut::from(&b"+OK\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();
    assert_eq!(frame, Some(Frame::Simple("OK".to_owned())));
    assert!(buffer.is_empty());
  }

  #[test]
  fn simple_string_returns_none_on_partial() {
    let mut buffer = BytesMut::from(&b"+OK\r"[..]);
    let frame = decode(&mut buffer).unwrap();
    assert_eq!(frame, None);
    assert_eq!(&buffer[..], b"+OK\r");
  }

  #[test]
  fn simple_string_leaves_remaining_bytes() {
    let mut buffer = BytesMut::from(&b"+OK\r\n+PONG\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();
    assert_eq!(frame, Some(Frame::Simple("OK".to_owned())));
    assert_eq!(&buffer[..], b"+PONG\r\n")
  }

  #[test]
  fn decodes_zero_integer() {
    let mut buffer = BytesMut::from(&b":0\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, Some(Frame::Integer(0)));
    assert!(buffer.is_empty());
  }

  #[test]
  fn decodes_positive_integer() {
    let mut buffer = BytesMut::from(&b":1000\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, Some(Frame::Integer(1000)));
    assert!(buffer.is_empty());
  }

  #[test]
  fn decodes_negative_integer() {
    let mut buffer = BytesMut::from(&b":-42\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, Some(Frame::Integer(-42)));
    assert!(buffer.is_empty());
  }

  #[test]
  fn decodes_integer_with_plus_sign() {
    let mut buffer = BytesMut::from(&b":+7\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, Some(Frame::Integer(7)));
    assert!(buffer.is_empty())
  }

  #[test]
  fn returns_none_on_partial_integer() {
    let mut buffer = BytesMut::from(&b":12\r"[..]);
    let result = decode(&mut buffer).unwrap();

    assert_eq!(result, None);
    assert_eq!(&buffer[..], b":12\r");
  }

  #[test]
  fn rejects_non_numeric_integer() {
    let mut buffer = BytesMut::from(&b":abc\r\n"[..]);
    let result = decode(&mut buffer);

    assert!(matches!(result, Err(DecodeError::InvalidInteger)));
  }

  #[test]
  fn rejects_empty_integer() {
    let mut buffer = BytesMut::from(&b":\r\n"[..]);
    let result = decode(&mut buffer);

    assert!(matches!(result, Err(DecodeError::InvalidInteger)));
  }

  #[test]
  fn rejects_integer_overflow() {
    let mut buffer = BytesMut::from(&b":9223372036854775808\r\n"[..]);
    let result = decode(&mut buffer);

    assert!(matches!(result, Err(DecodeError::InvalidInteger)));
  }

  #[test]
  fn leaves_remaining_bytes_after_integer() {
      let mut buffer = BytesMut::from(&b":5\r\n+OK\r\n"[..]);
      let frame = decode(&mut buffer).unwrap();

      assert_eq!(frame, Some(Frame::Integer(5)));
      assert_eq!(&buffer[..], b"+OK\r\n");
  }

  #[test]
  fn error_string_decodes_ok() {
    let mut buffer = BytesMut::from(&b"-ERR wrong\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, Some(Frame::Error("ERR wrong".to_owned())))
  }

  #[test]
  fn error_string_returns_none_on_partial() {
    let mut buffer = BytesMut::from(&b"-ERR wrong\r"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, None);
    assert_eq!(&buffer[..], b"-ERR wrong\r");
  }
  
  #[test]
  fn error_string_leaves_remaining_bytes() {
    let mut buffer = BytesMut::from(&b"-ERR wrong\r\n+OK\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, Some(Frame::Error("ERR wrong".to_owned())));
    assert_eq!(&buffer[..], b"+OK\r\n");
  }

}