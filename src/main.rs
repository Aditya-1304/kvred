use kvred::{db::state::new_shared_db, server::listener::run};

#[tokio::main]
pub async fn main() -> Result<(), std::io::Error> {
    let db = new_shared_db();

    run("127.0.0.1:6380", db).await
}