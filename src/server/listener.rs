use tokio::net::TcpListener;

use crate::{db::state::SharedDB, server::connection::handle_connection};

pub async fn run(addr: &str, db: SharedDB) -> std::io::Result<()> {
  let listener = TcpListener::bind(addr).await?;

  loop {
    let (socket, _) = listener.accept().await?;
    let db = db.clone();

    tokio::spawn(async move {
      let _ = handle_connection(socket, db).await;
    });
  }
}