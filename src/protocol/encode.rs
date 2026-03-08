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

