use std::{fs::OpenOptions, io::{self, Write}, path::{Path, PathBuf}};

use bytes::{Bytes, BytesMut};
use std::fs;

use crate::{db::types::Map, protocol::{encode::encode, frame::{ Frame}}};

pub fn rewrite_from_map(path: &Path, map: &Map) -> io::Result<()> {
  let tmp_path = temp_rewrite_path(path);

  let mut file = OpenOptions::new()
    .create(true)
    .write(true)
    .truncate(true)
    .open(&tmp_path)?;
  
  for (key, value) in map {
    let frame = Frame::Array(vec![
      Frame::Bulk(Bytes::from_static(b"SET")),
      Frame::Bulk(key.clone()),
      Frame::Bulk(value.clone()),
    ]);

    let mut buf = BytesMut::new();
    encode(&frame, &mut buf);
    file.write_all(&buf)?;
  }

  file.flush()?;
  file.sync_all()?;
  fs::rename(&tmp_path, path)?;

  Ok(())
}

fn temp_rewrite_path(path: &Path) -> PathBuf {
  let mut tmp = path.to_path_buf();
  tmp.set_extension("tmp");
  tmp
}