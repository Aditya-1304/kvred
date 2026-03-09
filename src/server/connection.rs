use bytes::BytesMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};

use crate::{command::{Command, exec::execute, parse::parse}, db::state::SharedStore, protocol::{decode::decode, encode::encode, frame::Frame}};

pub async fn handle_connection(stream: TcpStream, db: SharedStore) -> std::io::Result<()> {
  let mut stream = stream;
  let mut buffer = BytesMut::with_capacity(4096);

  loop {
    let n = stream.read_buf(&mut buffer).await?;
    if n == 0 {
      return Ok(());
    }

    loop {
      let frame = match decode(&mut buffer) {
        Ok(Some(frame)) => frame,
        Ok(None) => break,
        Err(_) => return Ok(()),
      };

      let cmd = match parse(frame) {
        Ok(cmd) => cmd,
        Err(_) => {
          let reply = Frame::Error("ERR invalid command".to_owned());
          let mut out = BytesMut::new();
          encode(&reply, &mut out);
          stream.write_all(&out).await?;
          continue;
        }
      };

      let reply = {
        let mut guard = db.lock().unwrap();

        if is_mutating(&cmd) {
          if guard.aof.append_command(&cmd).is_err() {
            Frame::Error("ERR persistence failure".to_owned())
          } else {
            execute(cmd, &mut guard.map)
          }
        } else {
        execute(cmd, &mut guard.map)
        }
      };

      let mut out = BytesMut::new();
      encode(&reply, &mut out);
      stream.write_all(&out).await?;
      
    }
  }
}

fn is_mutating(cmd: &Command) -> bool {
  matches!(cmd, Command::Set { .. } | Command::Del { .. })
}