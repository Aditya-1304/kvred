use tokio::sync::watch;

pub type ShutdownTx = watch::Sender<bool>;
pub type ShutdownRx = watch::Receiver<bool>;

pub fn channel() -> (ShutdownTx, ShutdownRx) {
  watch::channel(false)
}