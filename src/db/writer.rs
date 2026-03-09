use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::{command::{Command, exec::execute}, db::state::SharedMap, persistence::aof::Aof, protocol::frame::Frame};

pub enum WriteOper{
  Set { key: Bytes, value: Bytes},
  Del { keys: Vec<Bytes> },
}

pub struct WriteRequest {
  pub operation: WriteOper,
  pub response: oneshot::Sender<Frame>
}

pub fn spawn_writer(map: SharedMap, aof: Aof) -> mpsc::Sender<WriteRequest> {
  let (tx, mut rx) = mpsc::channel::<WriteRequest>(128);

  tokio::spawn(async move {
    let mut aof = aof;

    while let Some(req) = rx.recv().await {
        let cmd = match req.operation {
          WriteOper::Set { key, value } => Command::Set { key, value },
          WriteOper::Del { keys } => Command::Del { keys },
        };

        let reply = if aof.append_command(&cmd).is_err() {
          Frame::Error("ERR persistence failure".to_owned())
        } else {
          let mut guard = map.lock().unwrap();
          execute(cmd, &mut guard)
        };

        let _ = req.response.send(reply);
    }
  });
  tx
}