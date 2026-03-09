use bytes::BytesMut;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::oneshot};

use crate::{command::{Command, exec::execute, parse::parse}, db::{state::AppState, writer::{WriteOper, WriteRequest}}, protocol::{decode::decode, encode::encode, frame::Frame}};

pub async fn handle_connection(stream: TcpStream, state: AppState) -> std::io::Result<()> {
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

      let reply = match cmd {
        Command::Set { key, value } => {
          let (tx, rx) = oneshot::channel();

          if state
            .write_tx
            .send(WriteRequest {
              operation: WriteOper::Set { key, value },
              response: tx,
            })
            .await
            .is_err()
          {
            Frame::Error("ERR write path unavailable".to_owned())
          } else {
            match rx.await {
              Ok(reply) => reply,
              Err(_) => Frame::Error("ERR write path unavailable".to_owned()),
            }
          }
        }

        Command::Del { keys } => {
          let (tx, rx) = oneshot::channel();

          if state
            .write_tx
            .send(WriteRequest {
              operation: WriteOper::Del { keys },
              response: tx,
            })
            .await
            .is_err()
          {
            Frame::Error("ERR write path unavailable".to_owned())
          } else {
            match rx.await {
              Ok(reply) => reply,
              Err(_) => Frame::Error("ERR write path unavailable".to_owned()),
            }
          }
        }

        other => {
          let mut guard = state.map.lock().unwrap();
          execute(other, &mut guard)
        }
      };

      let mut out = BytesMut::new();
      encode(&reply, &mut out);
      stream.write_all(&out).await?;
      
    }
  }
}

