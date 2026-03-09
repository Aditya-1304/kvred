use std::{fs, io, path::Path};

use bytes::BytesMut;

use crate::{command::{Command, exec::execute, parse::parse}, db::types::Map, protocol::decode::decode};

pub fn replay_into(path: impl AsRef<Path>, map: &mut Map) -> io::Result<()> {
  let path = path.as_ref();
  
  if !path.exists() {
    return Ok(());
  }

  let bytes = fs::read(path)?;
  let mut buffer = BytesMut::from(bytes.as_slice());

  loop {
    match decode(&mut buffer) {
      Ok(Some(frame)) => {
        let cmd = parse(frame).map_err(|_| {
          io::Error::new(io::ErrorKind::InvalidData, "invalid command in aof")
        })?;

        match cmd {
          Command::Set { .. } | Command::Del { .. } => {
            let _ = execute(cmd, map);
          }
          _ => {
            return Err(io::Error::new(
              io::ErrorKind::InvalidData, 
              "non mutating command in aof"
          ));
          }
        }
      }

      Ok(None) => {
        if buffer.is_empty() {
          return Ok(());
        }

        return Err(io::Error::new(
          io::ErrorKind::UnexpectedEof, 
          "truncated aof",
        ));
      }

      Err(_) => {
        return Err(io::Error::new(
          io::ErrorKind::InvalidData,
          "malformed aof",
        ));
      }
    }
  }
}