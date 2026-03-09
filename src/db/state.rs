use std::sync::Arc;
use std::sync::Mutex;
use crate::db::types::Map;

pub type SharedDB = Arc<Mutex<Map>>;

pub fn new_shared_db() -> SharedDB {
  Arc::new(Mutex::new(Map::new()))
}
