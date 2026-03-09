use std::{
  fs,
  path::PathBuf,
  time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use kvred::{
  db::{
    state::{new_app_state, AppState},
    writer::{WriteOper, WriteRequest, WriterMsg},
  },
};
use tokio::sync::oneshot;

fn temp_aof_path(name: &str) -> PathBuf {
  let nanos = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_nanos();

  std::env::temp_dir().join(format!("kvred-rewrite-{name}-{nanos}.aof"))
}

async fn send_write(state: &AppState, operation: WriteOper) {
  let (tx, rx) = oneshot::channel();

  state
    .write_tx
    .send(WriterMsg::Write(WriteRequest {
        operation,
        response: tx,
    }))
    .await
    .unwrap();

  let reply = rx.await.unwrap();

  match reply {
    kvred::protocol::frame::Frame::Simple(_) | kvred::protocol::frame::Frame::Integer(_) => {}
    other => panic!("unexpected write reply: {:?}", other),
  }
}

async fn trigger_rewrite(state: &AppState) {
  let (tx, rx) = oneshot::channel();

  state
    .write_tx
    .send(WriterMsg::Rewrite { response: tx })
    .await
    .unwrap();

  rx.await.unwrap().unwrap();
}

fn cleanup_aof(path: PathBuf) {
  let _ = fs::remove_file(path);
}

#[tokio::test]
async fn rewrite_compacts_multiple_sets_to_same_key() {
  let path = temp_aof_path("compact-same-key");
  let (state, writer_handle) = new_app_state(&path).unwrap();

  send_write(
    &state,
    WriteOper::Set {
        key: Bytes::from_static(b"k"),
        value: Bytes::from_static(b"v1"),
    },
  )
  .await;

  send_write(
    &state,
    WriteOper::Set {
        key: Bytes::from_static(b"k"),
        value: Bytes::from_static(b"v2"),
    },
  )
  .await;

  trigger_rewrite(&state).await;

  drop(state);
  writer_handle.await.unwrap();

  let bytes = fs::read(&path).unwrap();

  assert_eq!(bytes, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nv2\r\n");

  cleanup_aof(path);
}

#[tokio::test]
async fn rewrite_omits_deleted_keys() {
  let path = temp_aof_path("omit-deleted");
  let (state, writer_handle) = new_app_state(&path).unwrap();

  send_write(
    &state,
    WriteOper::Set {
        key: Bytes::from_static(b"live"),
        value: Bytes::from_static(b"a"),
    },
  )
  .await;

  send_write(
    &state,
    WriteOper::Set {
        key: Bytes::from_static(b"gone"),
        value: Bytes::from_static(b"b"),
    },
  )
  .await;

  send_write(
    &state,
    WriteOper::Del {
        keys: vec![Bytes::from_static(b"gone")],
    },
  )
  .await;

  trigger_rewrite(&state).await;

  drop(state);
  writer_handle.await.unwrap();

  let bytes = fs::read(&path).unwrap();

  assert_eq!(
    bytes,
    b"*3\r\n$3\r\nSET\r\n$4\r\nlive\r\n$1\r\na\r\n"
  );

  cleanup_aof(path);
}

#[tokio::test]
async fn recovery_after_rewrite_restores_exact_state() {
  let path = temp_aof_path("recovery-after-rewrite");
  let (state, writer_handle) = new_app_state(&path).unwrap();

  send_write(
    &state,
    WriteOper::Set {
        key: Bytes::from_static(b"k1"),
        value: Bytes::from_static(b"v1"),
    },
  )
  .await;

  send_write(
    &state,
    WriteOper::Set {
        key: Bytes::from_static(b"k2"),
        value: Bytes::from_static(b"v2"),
    },
  )
  .await;

  send_write(
    &state,
    WriteOper::Del {
        keys: vec![Bytes::from_static(b"k1")],
    },
  )
  .await;

  trigger_rewrite(&state).await;

  drop(state);
  writer_handle.await.unwrap();

  let (recovered, recovered_writer) = new_app_state(&path).unwrap();

  let guard = recovered.map.lock().unwrap();

  assert!(guard.get(&Bytes::from_static(b"k1")).is_none());
  assert_eq!(
    guard.get(&Bytes::from_static(b"k2")),
    Some(&Bytes::from_static(b"v2"))
  );

  drop(guard);
  drop(recovered);
  recovered_writer.await.unwrap();

  cleanup_aof(path);
}
