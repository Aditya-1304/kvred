use std::{io, time::Duration};

use bytes::Bytes;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    command::{Command, exec::execute},
    config::FsyncPolicy,
    db::state::SharedMap,
    persistence::{aof::Aof, rewrite::rewrite_from_map},
    protocol::frame::Frame,
};

pub enum WriteOper {
    Set { key: Bytes, value: Bytes },
    Del { keys: Vec<Bytes> },
}

pub struct WriteRequest {
    pub operation: WriteOper,
    pub response: oneshot::Sender<Frame>,
}

pub enum WriterMsg {
    Write(WriteRequest),
    Rewrite {
        response: oneshot::Sender<io::Result<()>>,
    },
    Flush,
}

pub struct WriterHandles {
    pub writer: JoinHandle<()>,
    pub flusher: Option<JoinHandle<()>>,
    pub flush_stop: Option<oneshot::Sender<()>>,
}

pub fn spawn_writer(
    map: SharedMap,
    aof: Aof,
    policy: FsyncPolicy,
) -> (mpsc::Sender<WriterMsg>, WriterHandles) {
    let (tx, mut rx) = mpsc::channel::<WriterMsg>(128);

    let writer = tokio::task::spawn_blocking(move || {
        let mut aof = Some(aof);
        let mut dirty = false;

        while let Some(msg) = rx.blocking_recv() {
            match msg {
                WriterMsg::Write(req) => {
                    let WriteRequest {
                        operation,
                        response,
                    } = req;

                    let cmd = match operation {
                        WriteOper::Set { key, value } => Command::Set { key, value },
                        WriteOper::Del { keys } => Command::Del { keys },
                    };

                    let reply = match aof.as_mut() {
                        Some(current_aof) => match current_aof.append_command(&cmd) {
                            Ok(()) => match policy {
                                FsyncPolicy::Always => {
                                    if current_aof.flush_and_sync().is_err() {
                                        Frame::Error("ERR persistence failure".to_owned())
                                    } else {
                                        let mut guard = map.lock().unwrap();
                                        execute(cmd, &mut guard)
                                    }
                                }
                                FsyncPolicy::EverySec | FsyncPolicy::None => {
                                    dirty = true;
                                    let mut guard = map.lock().unwrap();
                                    execute(cmd, &mut guard)
                                }
                            },
                            Err(_) => Frame::Error("ERR persistence failure".to_owned()),
                        },
                        None => Frame::Error("ERR persistence unavailable".to_owned()),
                    };

                    let _ = response.send(reply);
                }

                WriterMsg::Rewrite { response } => {
                    let snapshot = {
                        let guard = map.lock().unwrap();
                        guard.clone()
                    };

                    let result = match aof.take() {
                        Some(mut current_aof) => {
                            let path = current_aof.path().to_path_buf();

                            let rewrite_result = (|| -> io::Result<Aof> {
                                current_aof.flush_and_sync()?;
                                drop(current_aof);

                                rewrite_from_map(&path, &snapshot)?;
                                Aof::open(&path)
                            })();

                            match rewrite_result {
                                Ok(new_aof) => {
                                    aof = Some(new_aof);
                                    dirty = false;
                                    Ok(())
                                }
                                Err(err) => {
                                    aof = Aof::open(&path).ok();
                                    Err(err)
                                }
                            }
                        }
                        None => Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "writer aof unavailable",
                        )),
                    };

                    let _ = response.send(result);
                }

                WriterMsg::Flush => {
                    if policy == FsyncPolicy::EverySec
                        && dirty
                        && let Some(current_aof) = aof.as_mut()
                        && current_aof.flush_and_sync().is_ok()
                    {
                        dirty = false;
                    }
                }
            }
        }

        if let Some(mut current_aof) = aof {
            let _ = current_aof.flush_and_sync();
        }
    });

    let (flush_stop, flusher) = if policy == FsyncPolicy::EverySec {
        let tx_clone = tx.clone();
        let (stop_tx, mut stop_rx) = oneshot::channel();

        let flusher = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        if tx_clone.send(WriterMsg::Flush).await.is_err() {
                            break;
                        }
                    }
                    _ = &mut stop_rx => break,
                }
            }
        });

        (Some(stop_tx), Some(flusher))
    } else {
        (None, None)
    };

    (
        tx,
        WriterHandles {
            writer,
            flusher,
            flush_stop,
        },
    )
}
