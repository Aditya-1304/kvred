use crate::{command::Command, db::types::Map, protocol::frame::Frame};

pub fn execute(cmd: Command, db: &mut Map) -> Frame {
    match cmd {
        Command::Ping(msg) => match msg {
            None => Frame::Simple("PONG".to_owned()),
            Some(msg) => Frame::Bulk(msg),
        },

        Command::Get { key } => match db.get(&key) {
            Some(value) => Frame::Bulk(value.clone()),
            None => Frame::NullBulk,
        },

        Command::Set { key, value } => {
            db.insert(key, value);
            Frame::Simple("OK".to_owned())
        }

        Command::Del { keys } => {
            let mut removed = 0;
            for key in keys {
                if db.remove(&key).is_some() {
                    removed += 1;
                }
            }
            Frame::Integer(removed)
        }

        Command::Exists { keys } => {
            let mut count = 0;
            for key in keys {
                if db.contains_key(&key) {
                    count += 1;
                }
            }
            Frame::Integer(count)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn ping_without_arg_returns_pong() {
        let mut db = Map::new();

        let frame = execute(Command::Ping(None), &mut db);

        assert_eq!(frame, Frame::Simple("PONG".to_owned()));
    }

    #[test]
    fn ping_with_arg_echoes_bulk() {
        let mut db = Map::new();

        let frame = execute(Command::Ping(Some(Bytes::from_static(b"hello"))), &mut db);

        assert_eq!(frame, Frame::Bulk(Bytes::from_static(b"hello")));
    }

    #[test]
    fn get_missing_returns_null_bulk() {
        let mut db = Map::new();

        let frame = execute(
            Command::Get {
                key: Bytes::from_static(b"missing"),
            },
            &mut db,
        );

        assert_eq!(frame, Frame::NullBulk);
    }

    #[test]
    fn set_then_get_returns_value() {
        let mut db = Map::new();

        let set_frame = execute(
            Command::Set {
                key: Bytes::from_static(b"mykey"),
                value: Bytes::from_static(b"myvalue"),
            },
            &mut db,
        );

        let get_frame = execute(
            Command::Get {
                key: Bytes::from_static(b"mykey"),
            },
            &mut db,
        );

        assert_eq!(set_frame, Frame::Simple("OK".to_owned()));
        assert_eq!(get_frame, Frame::Bulk(Bytes::from_static(b"myvalue")));
    }

    #[test]
    fn set_overwrites_existing_value() {
        let mut db = Map::new();

        execute(
            Command::Set {
                key: Bytes::from_static(b"mykey"),
                value: Bytes::from_static(b"first"),
            },
            &mut db,
        );

        execute(
            Command::Set {
                key: Bytes::from_static(b"mykey"),
                value: Bytes::from_static(b"second"),
            },
            &mut db,
        );

        let frame = execute(
            Command::Get {
                key: Bytes::from_static(b"mykey"),
            },
            &mut db,
        );

        assert_eq!(frame, Frame::Bulk(Bytes::from_static(b"second")));
    }

    #[test]
    fn del_single_existing_key_returns_1() {
        let mut db = Map::new();

        execute(
            Command::Set {
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
            },
            &mut db,
        );

        let frame = execute(
            Command::Del {
                keys: vec![Bytes::from_static(b"k1")],
            },
            &mut db,
        );

        assert_eq!(frame, Frame::Integer(1));
    }

    #[test]
    fn del_missing_key_returns_0() {
        let mut db = Map::new();

        let frame = execute(
            Command::Del {
                keys: vec![Bytes::from_static(b"missing")],
            },
            &mut db,
        );

        assert_eq!(frame, Frame::Integer(0));
    }

    #[test]
    fn del_multiple_keys_returns_count() {
        let mut db = Map::new();

        execute(
            Command::Set {
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
            },
            &mut db,
        );

        execute(
            Command::Set {
                key: Bytes::from_static(b"k2"),
                value: Bytes::from_static(b"v2"),
            },
            &mut db,
        );

        let frame = execute(
            Command::Del {
                keys: vec![
                    Bytes::from_static(b"k1"),
                    Bytes::from_static(b"k2"),
                    Bytes::from_static(b"missing"),
                ],
            },
            &mut db,
        );

        assert_eq!(frame, Frame::Integer(2));
    }

    #[test]
    fn exists_missing_returns_0() {
        let mut db = Map::new();

        let frame = execute(
            Command::Exists {
                keys: vec![Bytes::from_static(b"missing")],
            },
            &mut db,
        );

        assert_eq!(frame, Frame::Integer(0));
    }

    #[test]
    fn exists_existing_returns_1() {
        let mut db = Map::new();

        execute(
            Command::Set {
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
            },
            &mut db,
        );

        let frame = execute(
            Command::Exists {
                keys: vec![Bytes::from_static(b"k1")],
            },
            &mut db,
        );

        assert_eq!(frame, Frame::Integer(1));
    }

    #[test]
    fn exists_counts_duplicates() {
        let mut db = Map::new();

        execute(
            Command::Set {
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
            },
            &mut db,
        );

        let frame = execute(
            Command::Exists {
                keys: vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k1")],
            },
            &mut db,
        );

        assert_eq!(frame, Frame::Integer(2));
    }
}
