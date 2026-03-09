use std::io;

use tokio::net::TcpListener;

use crate::{db::state::SharedStore, server::connection::handle_connection};

pub async fn run(addr: &str, store: SharedStore) -> std::io::Result<()> {
  let listener = TcpListener::bind(addr).await?;

  serve(listener, store).await
  
}

pub async fn serve(listener: TcpListener, store: SharedStore) -> io::Result<()> {
  loop {
    let (socket, _) = listener.accept().await?;
    let store = store.clone();

    tokio::spawn(async move {
      let _ = handle_connection(socket, store).await;
    });
  }
}