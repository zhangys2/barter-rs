use crate::{
    engine::{
        Processor,
        audit::{AuditTick, Auditor, context::EngineContext},
        process_with_audit,
    },
    shutdown::SyncShutdown,
};
use barter_integration::{
    FeedEnded, Terminal,
    channel::{ChannelTxDroppable, MarketLatency, Tx},
};
use futures::{Stream, StreamExt};
use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
    time::Instant,
};
use tracing::info;

fn record_engine_process(process_latency: &Option<Arc<Mutex<MarketLatency>>>, started: Instant) {
    if let Some(samples) = process_latency
        && let Ok(mut samples) = samples.lock()
    {
        samples.engine_process.record(started);
    }
}

/// Synchronous `Engine` runner that processes input `Events`.
///
/// Runs until shutdown, returning an audit detailing the reason for the shutdown
/// (eg/ `Events` `FeedEnded`, `Command::Shutdown`, etc.).
///
/// # Arguments
/// * `Events` - `Iterator` of events for the `Engine` to process.
/// * `Engine` - Event processor that produces audit events as output.
/// * `process_latency` - Optional W5.4 Engine process hop samples.
pub fn sync_run<Events, Engine>(
    feed: &mut Events,
    engine: &mut Engine,
    process_latency: Option<Arc<Mutex<MarketLatency>>>,
) -> Engine::Audit
where
    Events: Iterator,
    Events::Item: Debug + Clone,
    Engine:
        Processor<Events::Item> + Auditor<Engine::Audit, Context = EngineContext> + SyncShutdown,
    Engine::Audit: From<FeedEnded> + Terminal + Debug,
{
    info!(
        feed_mode = "sync",
        audit_mode = "disabled",
        "Engine running"
    );

    // Run Engine process loop until shutdown
    let shutdown_audit = loop {
        let Some(event) = feed.next() else {
            break engine.audit(FeedEnded);
        };

        let started = Instant::now();
        let audit = process_with_audit(engine, event);
        record_engine_process(&process_latency, started);

        if audit.event.is_terminal() {
            break audit;
        }
    };

    info!(
        shutdown_audit = ?shutdown_audit.event,
        context = ?shutdown_audit.context,
        "Engine shutting down"
    );

    let _ = engine.shutdown();

    shutdown_audit.event
}

/// Synchronous `Engine` runner that processes input `Events` and forwards audits to the provided
/// `AuditTx`.
///
/// Runs until shutdown, returning an audit detailing the reason for the shutdown
/// (eg/ `Events` `FeedEnded`, `Command::Shutdown`, etc.).
///
/// # Arguments
/// * `Events` - `Iterator` of events for the `Engine` to process.
/// * `Engine` - Event processor that produces audit events as output.
/// * `AuditTx` - Channel for sending produced audit events.
/// * `process_latency` - Optional W5.4 Engine process hop samples.
pub fn sync_run_with_audit<Events, Engine, AuditTx>(
    feed: &mut Events,
    engine: &mut Engine,
    audit_tx: &mut ChannelTxDroppable<AuditTx>,
    process_latency: Option<Arc<Mutex<MarketLatency>>>,
) -> Engine::Audit
where
    Events: Iterator,
    Events::Item: Debug + Clone,
    Engine:
        Processor<Events::Item> + Auditor<Engine::Audit, Context = EngineContext> + SyncShutdown,
    Engine::Audit: From<FeedEnded> + Terminal + Debug + Clone,
    AuditTx: Tx<Item = AuditTick<Engine::Audit, EngineContext>>,
{
    info!(feed_mode = "sync", audit_mode = "enabled", "Engine running");

    let shutdown_audit = loop {
        let Some(event) = feed.next() else {
            break engine.audit(FeedEnded);
        };

        let started = Instant::now();
        let audit = process_with_audit(engine, event);
        record_engine_process(&process_latency, started);

        if audit.event.is_terminal() {
            break audit;
        }

        audit_tx.send(audit);
    };

    audit_tx.send(shutdown_audit.clone());

    info!(
        shutdown_audit = ?shutdown_audit.event,
        context = ?shutdown_audit.context,
        "Engine shutting down"
    );

    let _ = engine.shutdown();

    shutdown_audit.event
}

/// Asynchronous `Engine` runner that processes input `Events`.
///
/// Runs until shutdown, returning an audit detailing the reason for the shutdown
/// (eg/ `Events` `FeedEnded`, `Command::Shutdown`, etc.).
///
/// # Arguments
/// * `Events` - `Stream` of events for the `Engine` to process.
/// * `Engine` - Event processor that produces audit events as output.
/// * `process_latency` - Optional W5.4 Engine process hop samples.
pub async fn async_run<Events, Engine>(
    feed: &mut Events,
    engine: &mut Engine,
    process_latency: Option<Arc<Mutex<MarketLatency>>>,
) -> Engine::Audit
where
    Events: Stream + Unpin,
    Events::Item: Debug + Clone,
    Engine:
        Processor<Events::Item> + Auditor<Engine::Audit, Context = EngineContext> + SyncShutdown,
    Engine::Audit: From<FeedEnded> + Terminal + Debug,
{
    info!(
        feed_mode = "async",
        audit_mode = "disabled",
        "Engine running"
    );

    let shutdown_audit = loop {
        let Some(event) = feed.next().await else {
            break engine.audit(FeedEnded);
        };

        let started = Instant::now();
        let audit = process_with_audit(engine, event);
        record_engine_process(&process_latency, started);

        if audit.event.is_terminal() {
            break audit;
        }
    };

    info!(
        shutdown_audit = ?shutdown_audit.event,
        context = ?shutdown_audit.context,
        "Engine shutting down"
    );

    let _ = engine.shutdown();

    shutdown_audit.event
}

/// Asynchronous `Engine` runner that processes input `Events` and forwards audits to the provided
/// `AuditTx`.
///
/// Runs until shutdown, returning an audit detailing the reason for the shutdown
/// (eg/ `Events` `FeedEnded`, `Command::Shutdown`, etc.).
///
/// # Arguments
/// * `Events` - `Stream` of events for the `Engine` to process.
/// * `Engine` - Event processor that produces audit events as output.
/// * `AuditTx` - Channel for sending produced audit events.
/// * `process_latency` - Optional W5.4 Engine process hop samples.
pub async fn async_run_with_audit<Events, Engine, AuditTx>(
    feed: &mut Events,
    engine: &mut Engine,
    audit_tx: &mut ChannelTxDroppable<AuditTx>,
    process_latency: Option<Arc<Mutex<MarketLatency>>>,
) -> Engine::Audit
where
    Events: Stream + Unpin,
    Events::Item: Debug + Clone,
    Engine:
        Processor<Events::Item> + Auditor<Engine::Audit, Context = EngineContext> + SyncShutdown,
    Engine::Audit: From<FeedEnded> + Terminal + Debug + Clone,
    AuditTx: Tx<Item = AuditTick<Engine::Audit, EngineContext>>,
{
    info!(
        feed_mode = "async",
        audit_mode = "enabled",
        "Engine running"
    );

    let shutdown_audit = loop {
        let Some(event) = feed.next().await else {
            break engine.audit(FeedEnded);
        };

        let started = Instant::now();
        let audit = process_with_audit(engine, event);
        record_engine_process(&process_latency, started);

        if audit.event.is_terminal() {
            break audit;
        }

        audit_tx.send(audit);
    };

    audit_tx.send(shutdown_audit.clone());

    info!(
        shutdown_audit = ?shutdown_audit.event,
        context = ?shutdown_audit.context,
        "Engine shutting down"
    );

    let _ = engine.shutdown();

    shutdown_audit.event
}
