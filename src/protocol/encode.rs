use bytes::BytesMut;

use crate::protocol::frame::Frame;

pub fn encode(frame: &Frame, dest: &mut BytesMut) {
  match frame {
    Frame::Simple(s) => {
      dest.extend_from_slice(b"+");
      dest.extend_from_slice(s.as_bytes());
      dest.extend_from_slice(b"\r\n");
    },

    Frame::Error(s) => {
      dest.extend_from_slice(b"-");
      dest.extend_from_slice(s.as_bytes());
      dest.extend_from_slice(b"\r\n");
    },

    Frame::Integer(s) => {
      let text = s.to_string();
      dest.extend_from_slice(b":");
      dest.extend_from_slice(text.as_bytes());
      dest.extend_from_slice(b"\r\n");
    },

    Frame::Bulk(s) => {
      let len = s.len().to_string();
      dest.extend_from_slice(b"$");
      dest.extend_from_slice(len.as_bytes());
      dest.extend_from_slice(b"\r\n");
      dest.extend_from_slice(s.as_ref());
      dest.extend_from_slice(b"\r\n");
    }

    Frame::NullBulk => {
      dest.extend_from_slice(b"$-1\r\n");
    },

    Frame::NullArray => {
      dest.extend_from_slice(b"*-1\r\n");
    },

    Frame::Array(items) => {
      let len = items.len().to_string();
      dest.extend_from_slice(b"*");
      dest.extend_from_slice(len.as_bytes());
      dest.extend_from_slice(b"\r\n");

      for item in items {
        encode(item, dest);
      } 
    }

  }
}

#[cfg(test)]
mod test {
  
  use super::*;
  use bytes::Bytes;

  #[test]
  fn encodes_simple_string() {
    let mut dest = BytesMut::new();
    encode(&Frame::Simple("OK".to_owned()), &mut dest);
    assert_eq!(&dest[..], b"+OK\r\n");
  }

  #[test]
  fn encodes_error_string() {
    let mut dest = BytesMut::new();
    encode(&Frame::Error("ERR wrong".to_owned()), &mut dest);

    assert_eq!(&dest[..], b"-ERR wrong\r\n");
  }

  #[test]
  fn encodes_integer() {
    let mut dest = BytesMut::new();
    encode(&Frame::Integer(42), &mut dest);

    assert_eq!(&dest[..], b":42\r\n");
  }

  #[test]
  fn encodes_negative_integer() {
    let mut dest = BytesMut::new();
    encode(&Frame::Integer(-42), &mut dest);

    assert_eq!(&dest[..], b":-42\r\n");
  }

  #[test]
  fn encodes_bulk_string() {
    let mut dest = BytesMut::new();
    encode(&Frame::Bulk(Bytes::from_static(b"hello")), &mut dest);

    assert_eq!(&dest[..], b"$5\r\nhello\r\n");
  }

  #[test]
  fn encodes_empty_bulk_string() {
    let mut dest = BytesMut::new();
    encode(&Frame::Bulk(Bytes::from_static(b"")), &mut dest);

    assert_eq!(&dest[..], b"$0\r\n\r\n");
  }

  #[test]
  fn encodes_null_bulk() {
    let mut dest = BytesMut::new();
    encode(&Frame::NullBulk, &mut dest);

    assert_eq!(&dest[..], b"$-1\r\n");
  }

  #[test]
  fn encodes_empty_array() {
    let mut dest = BytesMut::new();
    encode(&Frame::Array(vec![]), &mut dest);

    assert_eq!(&dest[..], b"*0\r\n");
  }

  #[test]
  fn encodes_array_of_bulk_strings() {
    let mut dest = BytesMut::new();
    encode(
      &Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"hello")),
        Frame::Bulk(Bytes::from_static(b"world")),
      ]),
      &mut dest,
    );

    assert_eq!(&dest[..], b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
  }

  #[test]
  fn encodes_mixed_array() {
    let mut dest = BytesMut::new();
    encode(
      &Frame::Array(vec![
        Frame::Integer(1),
        Frame::Integer(2),
        Frame::Integer(3),
        Frame::Integer(4),
        Frame::Bulk(Bytes::from_static(b"hello")),
      ]),
      &mut dest,
    );

    assert_eq!(&dest[..], b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n");
  }

  #[test]
  fn encodes_nested_arrays() {
    let mut dest = BytesMut::new();
    encode(
      &Frame::Array(vec![
        Frame::Array(vec![
          Frame::Integer(1),
          Frame::Integer(2),
          Frame::Integer(3),
        ]),
        Frame::Array(vec![
          Frame::Simple("Hello".to_owned()),
          Frame::Error("World".to_owned()),
        ]),
      ]),
      &mut dest,
    );

    assert_eq!(
      &dest[..],
      b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n"
    );
  }

  #[test]
  fn encodes_null_array() {
    let mut dest = BytesMut::new();
    encode(&Frame::NullArray, &mut dest);

    assert_eq!(&dest[..], b"*-1\r\n");
  }

  #[test]
  fn encodes_array_with_null_bulk() {
    let mut dest = BytesMut::new();
    encode(
      &Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"hello")),
        Frame::NullBulk,
        Frame::Bulk(Bytes::from_static(b"world")),
      ]),
      &mut dest,
    );

    assert_eq!(&dest[..], b"*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n");
  }

}