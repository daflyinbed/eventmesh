//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to You under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.
//

//! HTTP webhook consumer embedded in the user's own axum application.
//!
//! This demonstrates the "tower middleware" mode: the user already has an
//! axum app and adds the webhook handler as a route. The SDK does not start
//! its own server.
//!
//! Assumes `docker compose --profile standalone up` is running (HTTP on
//! `127.0.0.1:10105`). Run the HTTP producer example in another terminal.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::{routing::post, Router};
use eventmesh::{
    config::HttpClientConfig,
    http::{HttpConsumer, WebhookHandler, WebhookState},
    model::{EventMeshMessage, SubscriptionItem, SubscriptionMode, SubscriptionType},
    MessageListener,
};

struct PrintingListener {
    count: AtomicU64,
}

impl MessageListener for PrintingListener {
    type Message = EventMeshMessage;

    async fn handle(&self, message: Self::Message) -> Option<Self::Message> {
        let n = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        println!(
            "[received #{n}] topic={:?} content={:?}",
            message.topic, message.content
        );
        None
    }
}

#[eventmesh::main]
async fn main() -> eventmesh::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let listener = Arc::new(PrintingListener {
        count: AtomicU64::new(0),
    });

    // Build the user's own axum app with the webhook handler embedded.
    let state = WebhookState::new(listener);
    let app = Router::new()
        .route("/my-eventmesh/callback", post(WebhookHandler::handle))
        .with_state(state);

    let webhook_url = "http://127.0.0.1:8080/my-eventmesh/callback";

    // Register the webhook URL with the EventMesh runtime.
    let config = HttpClientConfig::builder()
        .servers("127.0.0.1:10105")
        .env("env")
        .idc("idc")
        .sys("sys")
        .username("eventmesh")
        .password("eventmesh")
        .consumer_group("test-consumerGroup-http-mw")
        .build()?;

    let consumer = HttpConsumer::new(config)?;
    let items = vec![SubscriptionItem::new(
        "test-topic-rust-http",
        SubscriptionMode::CLUSTERING,
        SubscriptionType::ASYNC,
    )];
    consumer.subscribe_webhook(items, webhook_url).await?;
    println!("subscribed; serving webhook at {webhook_url} (Ctrl-C to stop)...");

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080")
        .await
        .map_err(eventmesh::EventMeshError::Io)?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await
        .map_err(|e| eventmesh::EventMeshError::Other(format!("server error: {e}")))?;

    consumer.shutdown().await;
    Ok(())
}
