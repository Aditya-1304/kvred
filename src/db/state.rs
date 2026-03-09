use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use crate::db::types::Map;
use crate::persistence::aof::Aof;

pub struct Store {
  pub map: Map, 
  pub aof: Aof,
}

pub type SharedStore = Arc<Mutex<Store>>;

pub fn new_shared_store(aof_path: impl AsRef<Path>) -> io::Result<SharedStore> {

  let aof = Aof::open(aof_path)?;
  let store = Store {
    map: Map::new(),
    aof,
  };

  Ok(Arc::new(Mutex::new(store)))

}
