use bytes::Bytes;
use tokio::{sync::{mpsc, oneshot}, task::JoinHandle};

use crate::{command::{Command, exec::execute}, db::state::SharedMap, persistence::{aof::Aof, rewrite::rewrite_from_map}, protocol::frame::Frame};

pub enum WriteOper{
  Set { key: Bytes, value: Bytes},
  Del { keys: Vec<Bytes> },
}

pub struct WriteRequest {
  pub operation: WriteOper,
  pub response: oneshot::Sender<Frame>
}

pub enum WriterMsg {
  Write(WriteRequest),
  Rewrite {
    response: oneshot::Sender<std::io::Result<()>>,
  }
}

pub fn spawn_writer(map: SharedMap, aof: Aof) -> (mpsc::Sender<WriterMsg>, JoinHandle<()>) {
  let (tx, mut rx) = mpsc::channel::<WriterMsg>(128);

  let handle = tokio::spawn(async move {
    let mut aof = aof;

    while let Some(msg) = rx.recv().await {
      match msg {
        WriterMsg::Write(req) => {

          let WriteRequest { operation, response} = req;
          let cmd = match operation {
            WriteOper::Set { key, value } => Command::Set { key, value },
            WriteOper::Del { keys } => Command::Del { keys },
          };

          let reply = if aof.append_command(&cmd).is_err() {
            Frame::Error("ERR persistence failure".to_owned())
          } else {
            let mut guard = map.lock().unwrap();
            execute(cmd, &mut guard)
          };

          let _ = response.send(reply);
        }
        WriterMsg::Rewrite { response } => {
          let result = {
            let guard = map.lock().unwrap();

            if let Err(err) = aof.flush_and_sync() {
              Err(err)
            } else if let Err(err) = rewrite_from_map(aof.path(), &guard) {
              Err(err)
            } else {
              Ok(())
            }
          };

          let result = result.and_then(|_| aof.reopen_append());

          let _ = response.send(result);
        }
      }
        
    }
    let _ = aof.flush_and_sync();
  });
  (tx, handle)
}