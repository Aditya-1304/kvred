use std::io;

use tokio::{net::TcpListener, task::JoinSet};

use crate::{db::state::AppState, server::{connection::handle_connection, shutdown::ShutdownRx}};

pub async fn run(addr: &str, state: AppState, shutdown: ShutdownRx) -> std::io::Result<()> {
  let listener = TcpListener::bind(addr).await?;

  serve(listener, state, shutdown).await
  
}

pub async fn serve(listener: TcpListener, state: AppState, mut shutdown: ShutdownRx) -> io::Result<()> {
  let mut tasks = JoinSet::new();

  loop {
    tokio::select! {
      res = listener.accept() => {
        let (socket, _) = res?;
        let state = state.clone();
        let shutdown_rx = shutdown.clone();

        tasks.spawn(async move {
          let _ = handle_connection(socket, state, shutdown_rx).await;
        });
      }

      _ = shutdown.changed() => {
        break;
      }
    }
  }

  while tasks.join_next().await.is_some() {}
  Ok(())

}