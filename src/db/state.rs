use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc;
use crate::db::types::Map;
use crate::db::writer::WriteRequest;
use crate::db::writer::spawn_writer;
use crate::persistence::aof::Aof;


pub type SharedMap = Arc<Mutex<Map>>;

#[derive(Clone)]
pub struct AppState {
  pub map: SharedMap, 
  pub write_tx: mpsc::Sender<WriteRequest>,
}

pub fn new_app_state(aof_path: impl AsRef<Path>) -> io::Result<AppState> {

let map = Arc::new(Mutex::new(Map::new()));
  let aof = Aof::open(aof_path)?;
  let write_tx = spawn_writer(map.clone(), aof);

  Ok(AppState { map, write_tx })

}
