use bytes::Bytes;
use std::str;

use crate::{command::Command, protocol::frame::Frame};

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
    match parts.len() {
      1 => Ok(Command::Ping(None)),
      2 => Ok(Command::Ping(Some(parts[1].clone()))),
      _ => Err(ParseError::WrongArity),
    }
  } else {
    Err(ParseError::UnknownCommand)
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