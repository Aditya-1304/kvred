use std::io;

use tokio::net::TcpListener;

use crate::{db::state::AppState, server::connection::handle_connection};

pub async fn run(addr: &str, state: AppState) -> std::io::Result<()> {
  let listener = TcpListener::bind(addr).await?;

  serve(listener, state).await
  
}

pub async fn serve(listener: TcpListener, state: AppState) -> io::Result<()> {
  loop {
    let (socket, _) = listener.accept().await?;
    let state = state.clone();

    tokio::spawn(async move {
      let _ = handle_connection(socket, state).await;
    });
  }
}