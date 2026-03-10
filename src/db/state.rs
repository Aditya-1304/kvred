use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot};

use crate::{
    config::FsyncPolicy,
    db::{
        types::Map,
        writer::{WriterHandles, WriterMsg, spawn_writer},
    },
    persistence::{aof::Aof, replay::replay_into},
};

pub type SharedMap = Arc<Mutex<Map>>;

#[derive(Clone)]
pub struct AppState {
    pub map: SharedMap,
    pub write_tx: mpsc::Sender<WriterMsg>,
}

pub fn new_app_state(
    aof_path: impl AsRef<Path>,
    policy: FsyncPolicy,
) -> io::Result<(AppState, WriterHandles)> {
    let aof_path = aof_path.as_ref();
    let mut initial_map = Map::new();
    replay_into(aof_path, &mut initial_map)?;

    let map = Arc::new(Mutex::new(initial_map));
    let aof = Aof::open(aof_path)?;
    let (write_tx, write_handles) = spawn_writer(map.clone(), aof, policy);

    Ok((AppState { map, write_tx }, write_handles))
}

pub async fn request_rewrite(state: &AppState) -> io::Result<()> {
    let (tx, rx) = oneshot::channel();

    state
        .write_tx
        .send(WriterMsg::Rewrite { response: tx })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer task stopped"))?;

    rx.await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer task stopped"))?
}
