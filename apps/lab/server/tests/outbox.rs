//! A dead outbox row is not delivered again until a human replays it.

use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use h5i_app_http::Auth;
use h5i_app_pg::{Engine, EngineConfig, pool};
use h5i_db_core::Database;
use h5i_db_service::Service;
use lab_kernel as k;
use lab_server::{
    App, Lab, LabStore, Runner, principal_from_token, replay_dead_outbox, router, token_user,
};
use tower::ServiceExt;

const NAME: &str = "h5idb-replay";
const PORT: &str = "5471";

struct Postgres;

impl Postgres {
    fn start() -> Self {
        let _ = Command::new("docker").args(["rm", "-f", NAME]).output();
        let started = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                NAME,
                "-e",
                "POSTGRES_USER=lab",
                "-e",
                "POSTGRES_PASSWORD=lab",
                "-e",
                "POSTGRES_DB=lab",
                "-p",
                &format!("{PORT}:5432"),
                "postgres:16",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("docker run");
        assert!(started.success(), "postgres container did not start");
        for _ in 0..60 {
            let ready = Command::new("docker")
                .args(["exec", NAME, "pg_isready", "-U", "lab", "-d", "lab"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or(false);
            if ready {
                return Postgres;
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        panic!("postgres was not ready");
    }
}

impl Drop for Postgres {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", NAME]).output();
    }
}

fn ledger_url() -> String {
    h5i_app_pg::with_schema(&format!("postgres://lab:lab@127.0.0.1:{PORT}/lab"), "lab")
        .expect("schema")
}

async fn post(app: App, token: &str) -> (axum::http::StatusCode, String) {
    let response = router(app)
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/api/outbox/replay")
                .header("authorization", format!("Bearer {token}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 2048)
        .await
        .unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test]
async fn a_human_replay_revives_only_dead_rows() {
    let _postgres = Postgres::start();
    let url = ledger_url();
    let mut connected = None;
    for _ in 0..60 {
        if let Ok((client, connection)) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await
        {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            connected = Some(client);
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let client = connected.expect("postgres accepted a connection");

    let engine = Engine::<Lab, LabStore>::new(pool(&url, 4).unwrap(), EngineConfig::default());
    engine.install_schema().await.unwrap();
    client
        .execute(
            "INSERT INTO h5i_outbox (tenant_id, dest, payload, attempts, dead, last_error)
             VALUES
               (1, 1, $1, 10, true, 'refused'),
               (1, 1, $2, 3, false, 'transient'),
               (1, 1, $3, 1, false, NULL)",
            &[&b"1:9".as_slice(), &b"1:2".as_slice(), &b"1:3".as_slice()],
        )
        .await
        .unwrap();
    client
        .execute(
            "UPDATE h5i_outbox SET delivered_at = now() WHERE payload = $1",
            &[&b"1:3".as_slice()],
        )
        .await
        .unwrap();

    let revived = replay_dead_outbox(&url).await.unwrap();
    assert_eq!(revived, 1);
    let dead: bool = client
        .query_one(
            "SELECT dead FROM h5i_outbox WHERE payload = $1",
            &[&b"1:9".as_slice()],
        )
        .await
        .unwrap()
        .get(0);
    let attempts: i32 = client
        .query_one(
            "SELECT attempts FROM h5i_outbox WHERE payload = $1",
            &[&b"1:9".as_slice()],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!dead);
    assert_eq!(attempts, 0);
    let kept: i32 = client
        .query_one(
            "SELECT attempts FROM h5i_outbox WHERE payload = $1",
            &[&b"1:2".as_slice()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(kept, 3);
    let delivered: bool = client
        .query_one(
            "SELECT delivered_at IS NOT NULL FROM h5i_outbox WHERE payload = $1",
            &[&b"1:3".as_slice()],
        )
        .await
        .unwrap()
        .get(0);
    assert!(delivered);

    client
        .execute(
            "UPDATE h5i_outbox SET dead = true, attempts = 10, last_error = 'again' WHERE payload = $1",
            &[&b"1:9".as_slice()],
        )
        .await
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Service::open(Database::create(&dir.path().join("db")).await.unwrap())
            .await
            .unwrap(),
    );
    let secret = b"replay-test".to_vec();
    let hmac = h5i_app_http::HmacAuth::<Lab>::new(secret.clone(), principal_from_token);
    let agent = hmac.issue(1, token_user(k::ROLE_AGENT, 4), 3_600);
    let human = hmac.issue(1, token_user(k::ROLE_HUMAN, 1), 3_600);
    let app = App {
        engine: Arc::new(engine),
        auth: Auth(Arc::new(hmac)),
        secret,
        db,
        runner: Arc::new(Mutex::new(Runner::new())),
        seal_id: "holdout".into(),
        holdout_table: "trades".into(),
        ledger_url: url.clone(),
    };

    let (status, _) = post(app.clone(), &agent).await;
    assert_eq!(status, axum::http::StatusCode::FORBIDDEN);
    let still_dead: bool = client
        .query_one(
            "SELECT dead FROM h5i_outbox WHERE payload = $1",
            &[&b"1:9".as_slice()],
        )
        .await
        .unwrap()
        .get(0);
    assert!(still_dead);

    let (status, body) = post(app.clone(), &human).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert!(body.contains("revived"), "{body}");
    let pending: bool = client
        .query_one(
            "SELECT NOT dead AND attempts = 0 FROM h5i_outbox WHERE payload = $1",
            &[&b"1:9".as_slice()],
        )
        .await
        .unwrap()
        .get(0);
    assert!(pending);

    let (status, body) = post(app, &human).await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert!(body.contains('0'), "{body}");
}
