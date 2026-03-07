use bytes::{Buf, Bytes, BytesMut};
use std::str;
use crate::protocol::frame::Frame;

#[derive(Debug)]
pub enum DecodeError {
  InvalidPrefix(u8),
  InvalidSimpleString,
  InvalidInteger,
  InvalidErrorString,
  InvalidBulkString

}

pub fn decode(buffer: &mut BytesMut) -> Result<Option<Frame>, DecodeError> {
  if buffer.is_empty() {
    return Ok(None);
  }

  match buffer[0] {
      b'+' => decode_simple_string(buffer),
      b':' => decode_integer(buffer),
      b'-' => decode_error_string(buffer),
      b'$' => decode_bulk_string(buffer),
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

pub fn decode_bulk_string(buffer: &mut BytesMut) -> Result<Option<Frame>, DecodeError> {
  let first_crlf = match find_crlf(buffer, 1) {
    Some(end) => end,
    None => return Ok(None),
  };

  let len = str::from_utf8(&buffer[1..first_crlf])
    .map_err(|_| DecodeError::InvalidBulkString)?
    .parse::<i64>()
    .map_err(|_| DecodeError::InvalidBulkString)?;

  if len == -1 {
    buffer.advance(first_crlf + 2);
    return Ok(Some(Frame::Null));
  }

  if len < -1 {
    return Err(DecodeError::InvalidBulkString);
  }

  let len = len as usize;
  let payload_start = first_crlf + 2;
  let payload_end = payload_start + len;
  let frame_end = payload_end + 2;

  if buffer.len() < frame_end {
    return Ok(None);
  }

  if&buffer[payload_end..frame_end] != b"\r\n" {
    return Err(DecodeError::InvalidBulkString);
  }


  let value = Bytes::copy_from_slice(&buffer[payload_start..payload_end]);

  buffer.advance(frame_end);
  Ok(Some(Frame::Bulk(value)))

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
  fn integer_decodes_zero() {
    let mut buffer = BytesMut::from(&b":0\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, Some(Frame::Integer(0)));
    assert!(buffer.is_empty());
  }

  #[test]
  fn integer_decodes_positive() {
    let mut buffer = BytesMut::from(&b":1000\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, Some(Frame::Integer(1000)));
    assert!(buffer.is_empty());
  }

  #[test]
  fn integer_decodes_negative() {
    let mut buffer = BytesMut::from(&b":-42\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, Some(Frame::Integer(-42)));
    assert!(buffer.is_empty());
  }

  #[test]
  fn integer_decodes_with_plus_sign() {
    let mut buffer = BytesMut::from(&b":+7\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, Some(Frame::Integer(7)));
    assert!(buffer.is_empty())
  }

  #[test]
  fn integer_returns_none_on_partial() {
    let mut buffer = BytesMut::from(&b":12\r"[..]);
    let result = decode(&mut buffer).unwrap();

    assert_eq!(result, None);
    assert_eq!(&buffer[..], b":12\r");
  }

  #[test]
  fn integer_rejects_non_numeric() {
    let mut buffer = BytesMut::from(&b":abc\r\n"[..]);
    let result = decode(&mut buffer);

    assert!(matches!(result, Err(DecodeError::InvalidInteger)));
  }

  #[test]
  fn integer_rejects_empty() {
    let mut buffer = BytesMut::from(&b":\r\n"[..]);
    let result = decode(&mut buffer);

    assert!(matches!(result, Err(DecodeError::InvalidInteger)));
  }

  #[test]
  fn integer_rejects_overflow() {
    let mut buffer = BytesMut::from(&b":9223372036854775808\r\n"[..]);
    let result = decode(&mut buffer);

    assert!(matches!(result, Err(DecodeError::InvalidInteger)));
  }

  #[test]
  fn integer_leaves_remaining_bytes() {
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

  #[test]
  fn bulk_string_decodes_ok() {
    let mut buffer = BytesMut::from(&b"$5\r\nhello\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, Some(Frame::Bulk(Bytes::from_static(b"hello"))));
    assert!(buffer.is_empty());
  }

  #[test]
  fn bulk_string_decodes_empty() {
    let mut buffer = BytesMut::from(&b"$0\r\n\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, Some(Frame::Bulk(Bytes::from_static(b""))));
    assert!(buffer.is_empty());
  }

  #[test]
  fn bulk_string_decodes_null() {
    let mut buffer = BytesMut::from(&b"$-1\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, Some(Frame::Null));
    assert!(buffer.is_empty());
  }

  #[test]
  fn bulk_string_returns_none_on_partial_length() {
    let mut buffer = BytesMut::from(&b"$5\r"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, None);
    assert_eq!(&buffer[..], b"$5\r");
  }

  #[test]
  fn bulk_string_returns_none_on_partial_payload() {
    let mut buffer = BytesMut::from(&b"$5\r\nhe"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, None);
    assert_eq!(&buffer[..], b"$5\r\nhe");
  }

  #[test]
  fn bulk_string_leaves_remaining_bytes() {
    let mut buffer = BytesMut::from(&b"$5\r\nhello\r\n+OK\r\n"[..]);
    let frame = decode(&mut buffer).unwrap();

    assert_eq!(frame, Some(Frame::Bulk(Bytes::from_static(b"hello"))));
    assert_eq!(&buffer[..], b"+OK\r\n");
  }

  #[test]
  fn bulk_string_rejects_invalid_length() {
    let mut buffer = BytesMut::from(&b"$abc\r\n"[..]);
    let result = decode(&mut buffer);

    assert!(matches!(result, Err(DecodeError::InvalidBulkString)));
  }

  #[test]
  fn bulk_string_rejects_negative_length_below_minus_one() {
    let mut buffer = BytesMut::from(&b"$-2\r\n"[..]);
    let result = decode(&mut buffer);

    assert!(matches!(result, Err(DecodeError::InvalidBulkString)));
  }

  #[test]
  fn bulk_string_rejects_missing_trailing_crlf() {
    let mut buffer = BytesMut::from(&b"$5\r\nhelloXX"[..]);
    let result = decode(&mut buffer);

    assert!(matches!(result, Err(DecodeError::InvalidBulkString)));
  }


}