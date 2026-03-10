use bytes::{Bytes, BytesMut};
use kvred::protocol::decode::decode;
use kvred::protocol::encode::encode;
use kvred::protocol::frame::Frame;

fn assert_roundtrip(frame: Frame) {
    let expected = frame.clone();
    let mut buf = BytesMut::new();

    encode(&frame, &mut buf);

    let decoded = decode(&mut buf).unwrap();

    assert_eq!(decoded, Some(expected));
    assert!(buf.is_empty());
}

#[test]
fn roundtrip_simple_string() {
    assert_roundtrip(Frame::Simple("OK".to_owned()));
}

#[test]
fn roundtrip_error_string() {
    assert_roundtrip(Frame::Error("ERR wrong".to_owned()));
}

#[test]
fn roundtrip_integer() {
    assert_roundtrip(Frame::Integer(42));
}

#[test]
fn roundtrip_negative_integer() {
    assert_roundtrip(Frame::Integer(-42));
}

#[test]
fn roundtrip_bulk_string() {
    assert_roundtrip(Frame::Bulk(Bytes::from_static(b"hello")));
}

#[test]
fn roundtrip_empty_bulk_string() {
    assert_roundtrip(Frame::Bulk(Bytes::from_static(b"")));
}

#[test]
fn roundtrip_null_bulk() {
    assert_roundtrip(Frame::NullBulk);
}

#[test]
fn roundtrip_empty_array() {
    assert_roundtrip(Frame::Array(vec![]));
}

#[test]
fn roundtrip_null_array() {
    assert_roundtrip(Frame::NullArray);
}

#[test]
fn roundtrip_array_of_bulk_strings() {
    assert_roundtrip(Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"hello")),
        Frame::Bulk(Bytes::from_static(b"world")),
    ]));
}

#[test]
fn roundtrip_mixed_array() {
    assert_roundtrip(Frame::Array(vec![
        Frame::Integer(1),
        Frame::Integer(2),
        Frame::Integer(3),
        Frame::Integer(4),
        Frame::Bulk(Bytes::from_static(b"hello")),
    ]));
}

#[test]
fn roundtrip_nested_arrays() {
    assert_roundtrip(Frame::Array(vec![
        Frame::Array(vec![
            Frame::Integer(1),
            Frame::Integer(2),
            Frame::Integer(3),
        ]),
        Frame::Array(vec![
            Frame::Simple("Hello".to_owned()),
            Frame::Error("World".to_owned()),
        ]),
    ]));
}

#[test]
fn roundtrip_array_with_null_bulk() {
    assert_roundtrip(Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"hello")),
        Frame::NullBulk,
        Frame::Bulk(Bytes::from_static(b"world")),
    ]));
}
