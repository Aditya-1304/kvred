use bytes::Bytes;
use std::str;

use crate::{command::Command, protocol::frame::Frame};

#[derive(Debug)]
pub enum ParseError {
  ExpectedArray,
  EmptyCommand,
  ExpectedBulkString,
  InvalidCommandName,
  UnknownCommand,
  WrongArity,
}

pub fn parse(frame: Frame) -> Result<Command, ParseError> {
  let parts = into_bulk_array(frame)?;

  let command_name = str::from_utf8(parts[0].as_ref())
    .map_err(|_| ParseError::InvalidCommandName)?;

  if command_name.eq_ignore_ascii_case("PING") {
    parse_ping(parts)
  } else if command_name.eq_ignore_ascii_case("GET") {
    parse_get(parts)
  } else if command_name.eq_ignore_ascii_case("SET") {
    parse_set(parts)
  } else if command_name.eq_ignore_ascii_case("DEL") {
    parse_del(parts)
  } else if command_name.eq_ignore_ascii_case("EXISTS") {
    parse_exists(parts)
  } else {
    Err(ParseError::UnknownCommand)
  }
}

pub fn parse_ping(parts: Vec<Bytes>) -> Result<Command, ParseError> {
  match parts.len() {
      1 => Ok(Command::Ping(None)),
      2 => Ok(Command::Ping(Some(parts[1].clone()))),
      _ => Err(ParseError::WrongArity),
  }
}

pub fn parse_get(parts: Vec<Bytes>) -> Result<Command, ParseError> {
  match parts.len() {
      2 => Ok(Command::Get { key: (parts[1].clone()) }),
      _ => Err(ParseError::WrongArity),
  }
}

pub fn parse_set(parts: Vec<Bytes>) -> Result<Command, ParseError> {
  match parts.len() {
      3 => Ok(Command::Set { 
        key: (parts[1].clone()), 
        value: (parts[2].clone()) 
      }),
      _ => Err(ParseError::WrongArity),
  }
}

pub fn parse_del(parts: Vec<Bytes>) -> Result<Command, ParseError> {
  match parts.len() {
    1 => Err(ParseError::WrongArity),
    _ => Ok(Command::Del { keys: (parts[1..].to_vec()) })
  }
}

pub fn parse_exists(parts: Vec<Bytes>) -> Result<Command, ParseError> {
  match parts.len() {
    1 => Err(ParseError::WrongArity),
    _ => Ok(Command::Exists { keys: (parts[1..].to_vec()) })
  }
}

pub fn into_bulk_array(frame: Frame) -> Result<Vec<Bytes>, ParseError> {
  match frame {
    Frame::Array(items) => {
      if items.is_empty() {
        return Err(ParseError::EmptyCommand);
      }

      let mut parts = Vec::with_capacity(items.len());

      for item in items {
        match item {
          Frame::Bulk(bytes) => parts.push(bytes),
          _ => return Err(ParseError::ExpectedBulkString),
        }
      }

      Ok(parts)
    }
    _ => Err(ParseError::ExpectedArray),
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use bytes::Bytes;

  #[test]
  fn parses_ping_without_args() {
    let frame = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"PING")),
    ]);

    let command = parse(frame).unwrap();

    assert_eq!(command, Command::Ping(None));
  }

  #[test]
  fn parses_ping_with_arg() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"PING")),
      Frame::Bulk(Bytes::from_static(b"hello")),
    ]);

    let command = parse(frame).unwrap();

    assert_eq!(command, Command::Ping(Some(Bytes::from_static(b"hello"))));
  }

  #[test]
  fn rejects_ping_with_too_many_args() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"PING")),
      Frame::Bulk(Bytes::from_static(b"hello")),
      Frame::Bulk(Bytes::from_static(b"world")),
    ]);

    let result = parse(frame);

    assert!(matches!(result, Err(ParseError::WrongArity)));
  }

  #[test]
  fn rejects_non_array_frame() {
    let frame = Frame::Simple("PING".to_owned());

    let result = parse(frame);

    assert!(matches!(result, Err(ParseError::ExpectedArray)));
  }

  #[test]
  fn rejects_array_with_non_bulk_elements() {
    let frame = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"PING")),
        Frame::Integer(1),
    ]);

    let result = parse(frame);

    assert!(matches!(result, Err(ParseError::ExpectedBulkString)));
  }

  #[test]
  fn rejects_empty_command_array() {
    let frame = Frame::Array(vec![]);

    let result = parse(frame);

    assert!(matches!(result, Err(ParseError::EmptyCommand)));
  }

  #[test]
  fn rejects_unknown_command() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"NOPE")),
    ]);

    let result = parse(frame);

    assert!(matches!(result, Err(ParseError::UnknownCommand)));
  }

  #[test]
  fn parses_ping_case_insensitively() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"pInG")),
    ]);

    let command = parse(frame).unwrap();

    assert_eq!(command, Command::Ping(None));
  }

  #[test]
  fn parses_get() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"GET")),
      Frame::Bulk(Bytes::from_static(b"mykey")),
    ]);

    let command = parse(frame).unwrap();

    assert_eq!(command, Command::Get { key: Bytes::from_static(b"mykey") });
  }

  #[test]
  fn rejects_get_without_key() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"GET")),
    ]);

    let result = parse(frame);

    assert!(matches!(result, Err(ParseError::WrongArity)));
  }

  #[test]
  fn rejects_get_with_too_many_args() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"GET")),
      Frame::Bulk(Bytes::from_static(b"k1")),
      Frame::Bulk(Bytes::from_static(b"k2")),
    ]);

    let result = parse(frame);

    assert!(matches!(result, Err(ParseError::WrongArity)));
  }

  #[test]
  fn parses_get_case_insensitively() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"gEt")),
      Frame::Bulk(Bytes::from_static(b"mykey")),
    ]);

    let command = parse(frame).unwrap();

    assert_eq!(command, Command::Get { key: Bytes::from_static(b"mykey") });
  }

  #[test]
  fn parses_set() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"SET")),
      Frame::Bulk(Bytes::from_static(b"mykey")),
      Frame::Bulk(Bytes::from_static(b"myvalue")),
    ]);

    let command = parse(frame).unwrap();

    assert_eq!(
      command,
      Command::Set {
        key: Bytes::from_static(b"mykey"),
        value: Bytes::from_static(b"myvalue"),
      }
    );
  }

  #[test]
  fn rejects_set_without_value() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"SET")),
      Frame::Bulk(Bytes::from_static(b"mykey")),
    ]);

    let result = parse(frame);

    assert!(matches!(result, Err(ParseError::WrongArity)));
  }

  #[test]
  fn rejects_set_with_too_many_args() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"SET")),
      Frame::Bulk(Bytes::from_static(b"mykey")),
      Frame::Bulk(Bytes::from_static(b"v1")),
      Frame::Bulk(Bytes::from_static(b"v2")),
    ]);

    let result = parse(frame);

    assert!(matches!(result, Err(ParseError::WrongArity)));
  }

  #[test]
  fn parses_set_case_insensitively() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"sEt")),
      Frame::Bulk(Bytes::from_static(b"mykey")),
      Frame::Bulk(Bytes::from_static(b"myvalue")),
    ]);

    let command = parse(frame).unwrap();

    assert_eq!(
      command,
      Command::Set {
        key: Bytes::from_static(b"mykey"),
        value: Bytes::from_static(b"myvalue"),
      }
    );
  }

  #[test]
  fn parses_del_single_key() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"DEL")),
      Frame::Bulk(Bytes::from_static(b"k1")),
    ]);

    let command = parse(frame).unwrap();

    assert_eq!(command, Command::Del { keys: vec![Bytes::from_static(b"k1")] });
  }

  #[test]
  fn parses_del_multiple_keys() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"DEL")),
      Frame::Bulk(Bytes::from_static(b"k1")),
      Frame::Bulk(Bytes::from_static(b"k2")),
    ]);

    let command = parse(frame).unwrap();

    assert_eq!(
      command,
      Command::Del { keys: vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")] }
    );
  }

  #[test]
  fn rejects_del_without_keys() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"DEL")),
    ]);

    let result = parse(frame);

    assert!(matches!(result, Err(ParseError::WrongArity)));
  }

  #[test]
  fn parses_del_case_insensitively() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"dEl")),
      Frame::Bulk(Bytes::from_static(b"k1")),
    ]);

    let command = parse(frame).unwrap();

    assert_eq!(command, Command::Del { keys: vec![Bytes::from_static(b"k1")] });
  }

  #[test]
  fn parses_exists_single_key() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"EXISTS")),
      Frame::Bulk(Bytes::from_static(b"k1")),
    ]);

    let command = parse(frame).unwrap();

    assert_eq!(command, Command::Exists { keys: vec![Bytes::from_static(b"k1")] });
  }

  #[test]
  fn parses_exists_multiple_keys() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"EXISTS")),
      Frame::Bulk(Bytes::from_static(b"k1")),
      Frame::Bulk(Bytes::from_static(b"k2")),
    ]);

    let command = parse(frame).unwrap();

    assert_eq!(
      command,
      Command::Exists { keys: vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")] }
    );
  }

  #[test]
  fn rejects_exists_without_keys() {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"EXISTS")),
    ]);

    let result = parse(frame);

    assert!(matches!(result, Err(ParseError::WrongArity)));
  }

  #[test]
  fn parses_exists_case_insensitively() {
    let frame = Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(b"eXiStS")),
        Frame::Bulk(Bytes::from_static(b"k1")),
    ]);

    let command = parse(frame).unwrap();

    assert_eq!(command, Command::Exists { keys: vec![Bytes::from_static(b"k1")] });
  }

}
