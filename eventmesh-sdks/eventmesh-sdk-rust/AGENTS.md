# AGENTS.md — EventMesh Rust SDK

Cargo crate `eventmesh` (`edition = "2021"`, **MSRV 1.75.0**). Speaks the
EventMesh **gRPC** protocol only (HTTP/TCP are stubbed as Phase 2/3 in
`Cargo.toml` but unimplemented). Wire format is CloudEvents-protobuf; the simple
`EventMeshMessage` model is converted at the gRPC boundary by `codec.rs`.

This crate is **not** part of the Gradle build and has **no GitHub Actions
CI**. All verification is local. The parent repo `AGENTS.md` covers the
docker-compose profiles and runtime ports; this file covers the Rust workflow.

## Build prerequisite: `protoc`

`build.rs` invokes `tonic-build`, which shells out to the `protoc` compiler at
**build time**. If `protoc` is not on `PATH`, every `cargo` command fails. The
README therefore prefixes commands with `PROTOC=$HOME/.local/bin/protoc`:

```bash
PROTOC=$HOME/.local/bin/protoc cargo build --features full
```

## Feature matrix

| Feature | Notes |
|---|---|
| `grpc` (default) | gRPC transport — essentially the whole SDK. Disable only for pure message-model use. |
| `cloud_events` | Native `cloudevents::Event` interop. |
| `tls` | TLS on the gRPC channel. |
| `full` | `grpc` + `cloud_events` + `tls`. Use this for clippy/test so every code path compiles. |
| `e2e` | Gates the live-server integration suite (`tests/e2e/`). A plain `cargo test` never touches Docker. |

## Verification (the order the README mandates)

```bash
PROTOC=$HOME/.local/bin/protoc cargo fmt
PROTOC=$HOME/.local/bin/protoc cargo clippy --features full --all-targets -- -D warnings
PROTOC=$HOME/.local/bin/protoc cargo test --features full
```

Clippy runs with **`-D warnings`** (warnings are errors here). There is no
`rustfmt.toml`/`clippy.toml` — defaults apply. To run a single test binary:
`cargo test --features full --test codec_test`.

## Generated proto code (do not hand-edit)

`build.rs` compiles `proto/eventmesh-{service,cloudevents}.proto` into `OUT_DIR`
at build time, generating **client stubs only** (`build_server(false)`), with
`--experimental_allow_proto3_optional`. The generated tree is pulled in via
`src/proto_gen.rs` → `tonic::include_proto!("...")`; it is **not** checked in.
Add convenience aliases in `proto_gen.rs`, not in the generated module.

## Architecture notes

- `src/lib.rs` is `#![deny(unsafe_code)]` — no `unsafe` anywhere.
- `src/transport/mod.rs` defines `Publisher` / `Subscriber` as **async-fn-in-trait**
  (Rust 1.75). They are therefore **not object-safe** — use the concrete
  `GrpcProducer` / `GrpcConsumer` directly, never `dyn`.
- `src/transport/grpc/codec.rs` is the `EventMeshMessage` ↔ CloudEvents-protobuf
  bridge; it is the only place wire encoding happens.
- `src/config/grpc.rs` — `GrpcClientConfig` + fluent builder (identity fields:
  `env`/`idc`/`sys`/`producer_group`/`consumer_group`/`username`/`password`/`token`).
- `src/common/` — `ProtocolKey`, status codes, constants used in gRPC headers/attrs.
- `#[eventmesh::main]` is just a re-export of `tokio::main`.

## End-to-end tests (`tests/e2e/`)

Run with: `PROTOC=$HOME/.local/bin/protoc cargo test --features e2e`.

- The harness (`tests/e2e/runtime.rs`) **auto-starts the `rocketmq` docker-compose
  profile** and tears it down at process exit. Set `EVENTMESH_E2E_EXTERNAL=1` to
  point at a server you started yourself.
- If neither Docker nor a reachable server is found, **tests skip, not fail**.
- Each test generates a unique topic + consumer group (monotonic counter + nanos
  timestamp), so the suite is parallel-safe on the shared broker.
- **Standalone (in-memory) broker limitations:** a topic must be created via the
  admin API **and** a consumer subscribed *before* any publish, and it does not
  implement request/reply. The harness warms topics automatically and the
  request/reply test self-skips its assertion on standalone — use the `rocketmq`
  profile (the e2e default) to exercise the full path.
- Topic creation hits the admin API at `POST /topic`, which expects
  **`application/x-www-form-urlencoded`** (not JSON).

## Conventions

- **Apache license header is required on every `.rs` file** — copy the header
  block from a neighbor (e.g. `build.rs`). Consistent with the rest of the repo.
- Builders use the `Option<T> field + fluent setter + consuming build()` idiom
  (see `GrpcClientConfigBuilder`); mirror it for new config types.
