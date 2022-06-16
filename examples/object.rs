use std::time::Duration;
use etcd_client::Client;
use tokio::time::sleep;
use etcdobj::SharedObject;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Testing {
    key: String,
}
impl Default for Testing {
    fn default() -> Self {
        Testing {
            key: "default".to_string(),
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let client = Client::connect(["localhost:2379"], None).await.unwrap();

    let foo = Testing {
        key: "hello world".to_string(),
    };

    let quark = SharedObject::new(
        client.clone(),
        "testing".to_string(),
        Some("foo2".to_string()),
        Some(foo),
    )
        .await;

    let mut quark = quark.unwrap();

    println!("abc");

    let _ = quark
        .update(|q| q.key = "updated value 3".to_string())
        .await;

    println!("123");

}
