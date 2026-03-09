use std::{
  fs,
  path::PathBuf,
  time::{SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use kvred::{
  db::state::new_app_state,
  protocol::{decode::decode, frame::Frame},
  server::{
    listener::serve,
    shutdown::{channel, ShutdownTx},
  },
};
use tokio::{
  io::{AsyncReadExt, AsyncWriteExt},
  net::{TcpListener, TcpStream},
  task::JoinHandle,
  time::{sleep, Duration},
};

fn temp_aof_path(name: &str) -> PathBuf {
  let nanos = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_nanos();

  std::env::temp_dir().join(format!("kvred-{name}-{nanos}.aof"))
}

async fn spawn_server(
    name: &str,
) -> (
  String,
  JoinHandle<std::io::Result<()>>,
  JoinHandle<()>,
  ShutdownTx,
  PathBuf,
) {
  let aof_path = temp_aof_path(name);
  let (state, writer_handle) = new_app_state(&aof_path).unwrap();
  let (shutdown_tx, shutdown_rx) = channel();

  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap().to_string();

  let listener_handle = tokio::spawn(async move {
    serve(listener, state, shutdown_rx).await
  });

  (addr, listener_handle, writer_handle, shutdown_tx, aof_path)
}

async fn stop_server(
  listener_handle: JoinHandle<std::io::Result<()>>,
  writer_handle: JoinHandle<()>,
  shutdown_tx: ShutdownTx,
) {
  let _ = shutdown_tx.send(true);

  let listener_result = listener_handle.await.unwrap();
  listener_result.unwrap();

  writer_handle.await.unwrap();
}

async fn connect_with_retry(addr: &str) -> TcpStream {
  for _ in 0..20 {
    match TcpStream::connect(addr).await {
      Ok(stream) => return stream,
      Err(_) => sleep(Duration::from_millis(25)).await,
    }
  }

  panic!("failed to connect to test server at {addr}");
}

async fn read_one_frame(stream: &mut TcpStream) -> Frame {
  let mut buf = BytesMut::with_capacity(1024);

  loop {
    match decode(&mut buf).unwrap() {
      Some(frame) => return frame,
      None => {
      let n = stream.read_buf(&mut buf).await.unwrap();
      assert!(n != 0, "server closed connection before sending a full frame");
      }
    }
  }
}

async fn send_and_read(stream: &mut TcpStream, request: &[u8]) -> Frame {
  stream.write_all(request).await.unwrap();
  read_one_frame(stream).await
}

fn cleanup_aof(aof_path: PathBuf) {
  let _ = fs::remove_file(aof_path);
}

#[tokio::test]
async fn ping_over_tcp() {
  let (addr, listener_handle, writer_handle, shutdown_tx, aof_path) =
    spawn_server("ping").await;

  let mut stream = connect_with_retry(&addr).await;
  let frame = send_and_read(&mut stream, b"*1\r\n$4\r\nPING\r\n").await;

  assert_eq!(frame, Frame::Simple("PONG".to_owned()));

  stop_server(listener_handle, writer_handle, shutdown_tx).await;
  cleanup_aof(aof_path);
}

#[tokio::test]
async fn set_then_get_over_tcp() {
  let (addr, listener_handle, writer_handle, shutdown_tx, aof_path) =
      spawn_server("set-get").await;

  let mut stream = connect_with_retry(&addr).await;

  let set_reply = send_and_read(
      &mut stream,
      b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nhello\r\n",
  )
  .await;

  let get_reply = send_and_read(
      &mut stream,
      b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n",
  )
  .await;

  assert_eq!(set_reply, Frame::Simple("OK".to_owned()));
  assert_eq!(get_reply, Frame::Bulk(Bytes::from_static(b"hello")));

  stop_server(listener_handle, writer_handle, shutdown_tx).await;
  cleanup_aof(aof_path);
}

#[tokio::test]
async fn unknown_command_returns_error() {
  let (addr, listener_handle, writer_handle, shutdown_tx, aof_path) =
    spawn_server("unknown").await;

  let mut stream = connect_with_retry(&addr).await;

  let frame = send_and_read(&mut stream, b"*2\r\n$4\r\nINCR\r\n$1\r\nx\r\n").await;

  assert_eq!(frame, Frame::Error("ERR invalid command".to_owned()));

  stop_server(listener_handle, writer_handle, shutdown_tx).await;
  cleanup_aof(aof_path);
}

#[tokio::test]
async fn set_over_tcp_is_appended_to_aof() {
    let (addr, listener_handle, writer_handle, shutdown_tx, aof_path) =
      spawn_server("set-aof").await;

    let mut stream = connect_with_retry(&addr).await;

    let reply = send_and_read(
      &mut stream,
      b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nhello\r\n",
    )
    .await;

    assert_eq!(reply, Frame::Simple("OK".to_owned()));

    stop_server(listener_handle, writer_handle, shutdown_tx).await;

    let bytes = fs::read(&aof_path).unwrap();

    assert_eq!(
      bytes,
      b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nhello\r\n"
    );

    cleanup_aof(aof_path);
}

#[tokio::test]
async fn del_over_tcp_is_appended_to_aof() {
  let (addr, listener_handle, writer_handle, shutdown_tx, aof_path) =
    spawn_server("del-aof").await;

  let mut stream = connect_with_retry(&addr).await;

  let set_reply = send_and_read(
    &mut stream,
    b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nhello\r\n",
  )
  .await;

  let del_reply = send_and_read(
    &mut stream,
    b"*2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n",
  )
  .await;

  assert_eq!(set_reply, Frame::Simple("OK".to_owned()));
  assert_eq!(del_reply, Frame::Integer(1));

  stop_server(listener_handle, writer_handle, shutdown_tx).await;

  let bytes = fs::read(&aof_path).unwrap();

  assert_eq!(
    bytes,
    b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nhello\r\n\
    *2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n"
  );

  cleanup_aof(aof_path);
}
