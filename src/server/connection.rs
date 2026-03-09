use bytes::BytesMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};

use crate::{command::{exec::execute, parse::parse}, db::state::SharedDB, protocol::{decode::decode, encode::encode, frame::{Frame}}};

pub async fn handle_connection(stream: TcpStream, db: SharedDB) -> std::io::Result<()> {
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
        execute(cmd, &mut guard)
      };

      let mut out = BytesMut::new();
      encode(&reply, &mut out);
      stream.write_all(&out).await?;
      
    }
  }
}