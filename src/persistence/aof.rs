use std::{
    fs::{File, OpenOptions},
    io::{self, Error, Write},
    path::{Path, PathBuf},
};

use bytes::{Bytes, BytesMut};

use crate::{
    command::Command,
    protocol::{encode::encode, frame::Frame},
};

pub struct Aof {
    path: PathBuf,
    file: File,
}

impl Aof {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = open_append_file(&path)?;

        Ok(Self { path, file })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append_command(&mut self, cmd: &Command) -> io::Result<()> {
        let frame = command_to_frame(cmd)
            .ok_or_else(|| Error::new(io::ErrorKind::InvalidInput, "command is not persistable"))?;

        let mut buffer = BytesMut::new();
        encode(&frame, &mut buffer);

        self.file.write_all(&buffer)?;
        Ok(())
    }

    pub fn flush_and_sync(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }
}

fn open_append_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)
}

fn command_to_frame(cmd: &Command) -> Option<Frame> {
    match cmd {
        Command::Set { key, value } => Some(Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"SET")),
            Frame::Bulk(key.clone()),
            Frame::Bulk(value.clone()),
        ])),
        Command::Del { keys } => {
            let mut items = vec![Frame::Bulk(Bytes::from_static(b"DEL"))];
            for key in keys {
                items.push(Frame::Bulk(key.clone()));
            }
            Some(Frame::Array(items))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_aof_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        std::env::temp_dir().join(format!("kvred-{name}-{nanos}.aof"))
    }

    #[test]
    fn set_is_appended_to_aof_in_resp_form() {
        let path = temp_aof_path("set");
        let mut aof = Aof::open(&path).unwrap();

        aof.append_command(&Command::Set {
            key: Bytes::from_static(b"mykey"),
            value: Bytes::from_static(b"hello"),
        })
        .unwrap();
        aof.flush_and_sync().unwrap();

        let bytes = fs::read(&path).unwrap();

        assert_eq!(bytes, b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nhello\r\n");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn del_is_appended_to_aof_in_resp_form() {
        let path = temp_aof_path("del");
        let mut aof = Aof::open(&path).unwrap();

        aof.append_command(&Command::Del {
            keys: vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")],
        })
        .unwrap();
        aof.flush_and_sync().unwrap();

        let bytes = fs::read(&path).unwrap();

        assert_eq!(bytes, b"*3\r\n$3\r\nDEL\r\n$2\r\nk1\r\n$2\r\nk2\r\n");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn append_command_rejects_non_mutating_command() {
        let path = temp_aof_path("reject");
        let mut aof = Aof::open(&path).unwrap();

        let err = aof.append_command(&Command::Ping(None)).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let _ = fs::remove_file(path);
    }
}
