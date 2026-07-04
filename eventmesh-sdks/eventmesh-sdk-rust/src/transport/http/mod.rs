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

//! HTTP transport for EventMesh.
//!
//! Provides an HTTP-based [`Publisher`](crate::transport::Publisher) and
//! [`Subscriber`](crate::transport::Subscriber), plus a tower-compatible
//! webhook middleware for receiving pushed messages from the EventMesh
//! runtime.
//!
//! # Wire format
//!
//! All requests use `application/x-www-form-urlencoded` bodies with JSON
//! payloads inside the `content` field, mirroring the Java SDK. The runtime
//! pushes messages to the consumer's registered webhook URL in the same
//! format, expecting a JSON reply `{"retCode": <int>}`.

pub mod client;
pub mod codec;
pub mod consumer;
pub mod producer;
pub mod server;
pub mod webhook;

pub use client::EventMeshHttpClient;
pub use consumer::HttpConsumer;
pub use producer::HttpProducer;
pub use server::WebhookServer;
pub use webhook::{WebhookHandler, WebhookLayer, WebhookState};
