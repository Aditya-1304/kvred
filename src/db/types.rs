use std::collections::HashMap;

use bytes::Bytes;

pub type Key = Bytes;
pub type Value = Bytes;
pub type Map = HashMap<Key, Value>;