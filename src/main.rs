use kvred::{
    config::FsyncPolicy,
    db::state::new_app_state,
    server::{listener::run, shutdown::channel},
};

#[tokio::main]
pub async fn main() -> Result<(), std::io::Error> {
    let policy = FsyncPolicy::from_env()?;
    let (state, writer_handles) = new_app_state("kvred.aof", policy)?;
    let (shutdown_tx, shutdown_rx) = channel();

    let listener_handle =
        tokio::spawn(async move { run("127.0.0.1:6380", state, shutdown_rx).await });

    tokio::signal::ctrl_c()
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let _ = shutdown_tx.send(true);

    let listener_result = listener_handle
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    listener_result?;

    if let Some(stop) = writer_handles.flush_stop {
        let _ = stop.send(());
    }

    if let Some(flusher) = writer_handles.flusher {
        flusher
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    }

    writer_handles
        .writer
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    Ok(())
}
