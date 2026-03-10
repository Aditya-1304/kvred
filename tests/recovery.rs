use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use kvred::{
    command::Command,
    config::FsyncPolicy,
    db::{state::new_app_state, writer::WriterHandles},
    persistence::aof::Aof,
};

async fn stop_writer(handles: WriterHandles) {
    if let Some(stop) = handles.flush_stop {
        let _ = stop.send(());
    }

    if let Some(flusher) = handles.flusher {
        flusher.await.unwrap();
    }

    handles.writer.await.unwrap();
}

fn temp_aof_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    std::env::temp_dir().join(format!("kvred-recovery-{name}-{nanos}.aof"))
}

#[tokio::test]
async fn recovery_replays_set_command() {
    let path = temp_aof_path("set");

    let mut aof = Aof::open(&path).unwrap();
    aof.append_command(&Command::Set {
        key: Bytes::from_static(b"mykey"),
        value: Bytes::from_static(b"hello"),
    })
    .unwrap();
    aof.flush_and_sync().unwrap();

    let (state, writer_handles) = new_app_state(&path, FsyncPolicy::Always).unwrap();

    let guard = state.map.lock().unwrap();
    assert_eq!(
        guard.get(&Bytes::from_static(b"mykey")),
        Some(&Bytes::from_static(b"hello"))
    );

    drop(guard);
    drop(state);
    stop_writer(writer_handles).await;
    let _ = fs::remove_file(path);
}

#[tokio::test]
async fn recovery_replays_set_and_del() {
    let path = temp_aof_path("set-del");

    let mut aof = Aof::open(&path).unwrap();
    aof.append_command(&Command::Set {
        key: Bytes::from_static(b"mykey"),
        value: Bytes::from_static(b"hello"),
    })
    .unwrap();

    aof.append_command(&Command::Del {
        keys: vec![Bytes::from_static(b"mykey")],
    })
    .unwrap();

    aof.flush_and_sync().unwrap();

    let (state, writer_handles) = new_app_state(&path, FsyncPolicy::Always).unwrap();

    let guard = state.map.lock().unwrap();
    assert!(guard.get(&Bytes::from_static(b"mykey")).is_none());

    drop(guard);
    drop(state);
    stop_writer(writer_handles).await;
    let _ = fs::remove_file(path);
}

#[tokio::test]
async fn recovery_preserves_last_value() {
    let path = temp_aof_path("last-value");

    let mut aof = Aof::open(&path).unwrap();
    aof.append_command(&Command::Set {
        key: Bytes::from_static(b"k"),
        value: Bytes::from_static(b"v1"),
    })
    .unwrap();

    aof.append_command(&Command::Set {
        key: Bytes::from_static(b"k"),
        value: Bytes::from_static(b"v2"),
    })
    .unwrap();

    aof.flush_and_sync().unwrap();

    let (state, writer_handles) = new_app_state(&path, FsyncPolicy::Always).unwrap();

    let guard = state.map.lock().unwrap();
    assert_eq!(
        guard.get(&Bytes::from_static(b"k")),
        Some(&Bytes::from_static(b"v2"))
    );

    drop(guard);
    drop(state);
    stop_writer(writer_handles).await;
    let _ = fs::remove_file(path);
}
