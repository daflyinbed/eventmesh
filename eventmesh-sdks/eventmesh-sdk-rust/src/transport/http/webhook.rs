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

//! Webhook middleware for receiving pushed messages from the EventMesh runtime.
//!
//! The runtime POSTs messages to the consumer's registered webhook URL using
//! `application/x-www-form-urlencoded` bodies. This module provides:
//!
//! - [`WebhookHandler`] — an axum handler/extractor that parses the push body,
//!   dispatches to a [`MessageListener`], and returns the JSON acknowledgment.
//! - [`WebhookLayer`] — a convenience wrapper that produces the
//!   [`WebhookState`] for an axum `Router::with_state` call.
//! - [`WebhookState`] — shared state holding the listener, passed via axum's
//!   `State` extractor.
//!
//! # Example (axum)
//!
//! ```no_run
//! # use eventmesh::{
//! #     config::HttpClientConfig,
//! #     http::{HttpConsumer, WebhookHandler, WebhookState},
//! #     model::{EventMeshMessage, SubscriptionItem, SubscriptionMode, SubscriptionType},
//! #     MessageListener,
//! # };
//! # use axum::{Router, routing::post};
//! # use std::sync::Arc;
//! # struct MyListener;
//! # impl MessageListener for MyListener {
//! #     type Message = EventMeshMessage;
//! #     async fn handle(&self, _: Self::Message) -> Option<Self::Message> { None }
//! # }
//! # #[eventmesh::main]
//! # async fn main() -> eventmesh::Result<()> {
//! let listener = Arc::new(MyListener);
//! let state = WebhookState::new(listener);
//! let app: Router = Router::new()
//!     .route("/eventmesh/callback", post(WebhookHandler::handle))
//!     .with_state(state);
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::Json;
use bytes::Bytes;
use tracing::{debug, error, warn};

use crate::model::EventMeshMessage;
use crate::transport::http::codec::{parse_push_body, WebhookReply};
use crate::MessageListener;

/// Shared state for the webhook handler, holding the message listener.
pub struct WebhookState<L: MessageListener<Message = EventMeshMessage>> {
    listener: Arc<L>,
}

impl<L: MessageListener<Message = EventMeshMessage>> WebhookState<L> {
    /// Create state wrapping the given listener.
    pub fn new(listener: Arc<L>) -> Self {
        Self { listener }
    }

    /// Access the inner listener.
    pub fn listener(&self) -> &L {
        &self.listener
    }
}

impl<L: MessageListener<Message = EventMeshMessage>> Clone for WebhookState<L> {
    fn clone(&self) -> Self {
        Self {
            listener: Arc::clone(&self.listener),
        }
    }
}

/// Axum handler that processes an EventMesh webhook push.
///
/// Register it on a route:
///
/// ```ignore
/// Router::new()
///     .route("/cb", post(WebhookHandler::handle))
///     .with_state(state);
/// ```
pub struct WebhookHandler;

impl WebhookHandler {
    /// The actual handler function. Extracts the body bytes, parses the
    /// form-urlencoded push body, dispatches to the listener, and returns the
    /// JSON acknowledgment `{"retCode": <int>}`.
    pub async fn handle<L: MessageListener<Message = EventMeshMessage>>(
        State(state): State<WebhookState<L>>,
        _headers: HeaderMap,
        body: Bytes,
    ) -> impl IntoResponse {
        let body_str = match std::str::from_utf8(&body) {
            Ok(s) => s,
            Err(e) => {
                warn!("webhook body not UTF-8: {e}");
                return Json(WebhookReply::retry("invalid UTF-8")).into_response();
            }
        };

        let push_body = match parse_push_body(body_str) {
            Ok(b) => b,
            Err(e) => {
                warn!("webhook body parse error: {e}");
                return Json(WebhookReply::retry("form decode error")).into_response();
            }
        };

        let msg = match push_body.to_event_mesh_message() {
            Ok(m) => m,
            Err(e) => {
                error!("webhook message decode error: {e}");
                return Json(WebhookReply::retry("message decode error")).into_response();
            }
        };

        debug!(
            "webhook received topic={:?} bizseqno={:?}",
            msg.topic, msg.biz_seq_no
        );

        match state.listener.handle(msg).await {
            Some(reply) => {
                // The listener produced a reply, but the HTTP webhook transport
                // cannot deliver it: the runtime's protocol adaptor does not
                // support REPLY_MESSAGE (code 301) on the CloudEvents path, so
                // there is no wire path to route the reply back to the original
                // requester. SYNC subscriptions are rejected at subscribe time;
                // this warning is a defensive backstop for messages pushed from
                // a non-Rust consumer or a legacy subscription.
                warn!(
                    "listener produced a reply (topic={:?}) but the HTTP webhook \
                     transport cannot deliver replies; use the gRPC transport for \
                     request/reply",
                    reply.topic
                );
                Json(WebhookReply::ok()).into_response()
            }
            None => Json(WebhookReply::ok()).into_response(),
        }
    }
}

/// A convenience wrapper that produces [`WebhookState`] for an axum
/// `Router::with_state` call.
///
/// Despite the name, this is **not** a `tower::Layer` — it does not wrap a
/// service. It is a thin builder that bridges a [`MessageListener`] into the
/// [`WebhookState`] consumed by [`WebhookHandler`]. In most cases you'll just
/// register the [`WebhookHandler`] on an axum route.
pub struct WebhookLayer<L: MessageListener<Message = EventMeshMessage>> {
    listener: Arc<L>,
}

impl<L: MessageListener<Message = EventMeshMessage>> WebhookLayer<L> {
    /// Create a layer wrapping the given listener.
    pub fn new(listener: Arc<L>) -> Self {
        Self { listener }
    }

    /// Build the [`WebhookState`] for use with an axum `Router::with_state`.
    pub fn into_state(self) -> WebhookState<L> {
        WebhookState {
            listener: self.listener,
        }
    }
}
