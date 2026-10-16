//! Fleet console. The ledger listens on `LISTEN` (default `127.0.0.1:8201`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use h5i_app_http::Auth;
use h5i_app_pg::{Engine, EngineConfig, pool};
use h5i_db_core::Database;
use h5i_db_service::Service;
use lab_kernel as k;
use lab_server::{App, EffectDeliver, Lab, LabStore, Runner, principal_from_token, token_user};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let url = std::env::var("DATABASE_URL")?;
    let secret = std::env::var("H5I_APP_SECRET")?;
    let db_path = std::env::var("H5I_DB_PATH")?;
    let addr = std::env::var("LISTEN").unwrap_or_else(|_| "127.0.0.1:8201".into());
    let seal_id = std::env::var("LAB_SEAL").unwrap_or_else(|_| "holdout".into());
    let holdout_table = std::env::var("LAB_TABLE").unwrap_or_else(|_| "trades".into());

    let hmac = h5i_app_http::HmacAuth::<Lab>::new(secret, principal_from_token);
    if let Ok(spec) = std::env::var("H5I_APP_ISSUE") {
        // H5I_APP_ISSUE=<org>:<role>:<id> prints a one-day token and exits.
        let mut parts = spec.split(':');
        let org: u64 = parts.next().ok_or("H5I_APP_ISSUE=org:role:id")?.parse()?;
        let role: u64 = parts.next().ok_or("role")?.parse()?;
        let id: u64 = parts.next().ok_or("id")?.parse()?;
        println!("{}", hmac.issue(org, token_user(role, id), 86_400));
        return Ok(());
    }

    let engine = Arc::new(Engine::<Lab, LabStore>::new(
        pool(&h5i_app_pg::with_schema(&url, "lab")?, 8)?,
        EngineConfig::default(),
    ));
    engine.install_schema().await?;
    let db = Arc::new(Service::open(Database::open(db_path.as_ref()).await?).await?);
    let app = App {
        engine: Arc::clone(&engine),
        auth: Auth(Arc::new(hmac)),
        db,
        alerts: Arc::new(Mutex::new(Vec::new())),
        runner: Arc::new(Mutex::new(Runner::new())),
        seal_id,
        holdout_table,
    };
    let mut registry = HashMap::new();
    registry.insert(k::DEST_DB, ());
    let dispatcher = engine.dispatcher(
        registry,
        EffectDeliver { app: app.clone() },
        Default::default(),
    );
    let router = lab_server::router(app);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "listening");
    tokio::spawn(async move {
        loop {
            if let Err(error) = dispatcher.run_once().await {
                tracing::error!(%error, "outbox pass");
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    });
    axum::serve(listener, router).await?;
    Ok(())
}
