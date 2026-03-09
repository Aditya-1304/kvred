use kvred::{db::state::new_app_state, server::listener::run};

#[tokio::main]
pub async fn main() -> Result<(), std::io::Error> {
    let db = new_app_state("kvred.aof")?;
    
    run("127.0.0.1:6380", db).await
}