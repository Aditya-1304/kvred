use kvred::{db::state::new_app_state, server::{listener::run, shutdown::channel}};

#[tokio::main]
pub async fn main() -> Result<(), std::io::Error> {
    let (state, writer_handle) = new_app_state("kvred.aof")?;
    let (shutdown_tx, shutdown_rx) = channel();

    let listener_handle = tokio::spawn(async move {
        run("127.0.0.1:6380", state, shutdown_rx).await
    });

    tokio::signal::ctrl_c().await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let _ = shutdown_tx.send(true);

    let listener_result = listener_handle
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    listener_result?;

    writer_handle
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    Ok(())
}
