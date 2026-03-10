use std::{env, io};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    Always,
    EverySec,
    None,
}

impl FsyncPolicy {
    pub fn from_env() -> io::Result<Self> {
        let raw = env::var("KVRED_FSYNC").unwrap_or_else(|_| "always".to_owned());

        match raw.as_str() {
            "always" => Ok(Self::Always),
            "everysec" => Ok(Self::EverySec),
            "none" => Ok(Self::None),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid KVRED_FSYNC value: {other}"),
            )),
        }
    }
}
