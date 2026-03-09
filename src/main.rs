use kvred::{db::state::new_shared_store, server::listener::run};

#[tokio::main]
pub async fn main() -> Result<(), std::io::Error> {
    let db = new_shared_store("kvred.aof")?;
    
    run("127.0.0.1:6380", db).await
}