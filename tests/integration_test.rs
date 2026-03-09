use bytes::{Bytes, BytesMut};
use kvred::{
  db::state::new_shared_db,
  protocol::{decode::decode, frame::Frame},
  server::listener::run,
};
use tokio::{
  io::{AsyncReadExt, AsyncWriteExt},
  net::TcpStream,
  task::JoinHandle,
  time::{sleep, Duration},
};

fn spawn_server(addr: &'static str) -> JoinHandle<()> {
  let db = new_shared_db();

  tokio::spawn(async move {
    let _ = run(addr, db).await;
  })
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

#[tokio::test]
async fn ping_over_tcp() {
  let addr = "127.0.0.1:6381";
  let server = spawn_server(addr);

  let mut stream = connect_with_retry(addr).await;

  let frame = send_and_read(&mut stream, b"*1\r\n$4\r\nPING\r\n").await;

  assert_eq!(frame, Frame::Simple("PONG".to_owned()));

  server.abort();
}

#[tokio::test]
async fn set_then_get_over_tcp() {
  let addr = "127.0.0.1:6382";
  let server = spawn_server(addr);

  let mut stream = connect_with_retry(addr).await;

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

  server.abort();
}

#[tokio::test]
async fn unknown_command_returns_error() {
  let addr = "127.0.0.1:6383";
  let server = spawn_server(addr);

  let mut stream = connect_with_retry(addr).await;

  let frame = send_and_read(
      &mut stream,
      b"*2\r\n$4\r\nINCR\r\n$1\r\nx\r\n",
  )
  .await;

  assert_eq!(frame, Frame::Error("ERR invalid command".to_owned()));

  server.abort();
}
