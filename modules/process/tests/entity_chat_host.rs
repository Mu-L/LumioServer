//! Consume-only host unit tests. Binding/query truth is the Runtime double.

mod common;

use common::{runtime_wire_chat_input, SharedRuntime, TestKernel, RUNTIME_WIRE_CHAT_INPUT};
use lumio_host_runtime::{bounded_channel, HostClock, SharedClock};
use lumio_server_process::entity_chat::{
    generate_keys, issue_admission_credential, AttributeQueryOutcome, AttributeQueryRequest,
    AttributeQueryScope, BoundEntityKind, ChatOpKind, EntityChatHost, QueryResult,
    ADMISSION_KEY_ID, MAX_CHAT_INPUTS_PER_TICK, MAX_DEFERRED_FRAMES_PER_CONNECTION,
    MAX_PENDING_ADMISSIONS, MAX_PENDING_QUERIES, RECONNECT_WINDOW_MS,
};
use std::sync::Arc;

fn host_with(
    runtime: SharedRuntime,
) -> (
    EntityChatHost,
    lumio_server_process::entity_chat::Ed25519KeyPair,
) {
    let keys = generate_keys();
    let host = EntityChatHost::new(
        RECONNECT_WINDOW_MS,
        SharedClock::test(),
        Box::new(runtime),
        Box::new(TestKernel::new()),
        ADMISSION_KEY_ID,
        keys.public.to_vec(),
        1_000,
    );
    (host, keys)
}

fn credential(
    keys: &lumio_server_process::entity_chat::Ed25519KeyPair,
    name: &str,
    bot: bool,
) -> String {
    issue_admission_credential(&keys.seed, 1, &format!("acct_{name}"), name, bot, 1, 9_000)
}

#[test]
fn username_password_is_never_an_admission_path() {
    let (host, _) = host_with(SharedRuntime::new());
    assert!(!host.try_admit_username_password("room-main", "c1", "Bot01", "123456"));
}

#[test]
fn admit_creates_bot_and_player_and_resolves_bindings() {
    let (host, keys) = host_with(SharedRuntime::new());
    let bot = host.admit(
        "room-main".to_owned(),
        "c-bot01".to_owned(),
        credential(&keys, "Bot01", true),
    );
    let player = host.admit(
        "room-main".to_owned(),
        "c-browser".to_owned(),
        credential(&keys, "Browser01", false),
    );
    assert!(bot.accepted && player.accepted);
    assert_eq!(host.must_self("c-bot01").entity_type, BoundEntityKind::Bot);
    assert_eq!(
        host.must_self("c-browser").entity_type,
        BoundEntityKind::Player
    );
    let self_bot = host.must_self("c-bot01");
    assert!(host
        .try_resolve_by_net_entity_id("room-main".to_owned(), self_bot.net_entity_id)
        .expect("Runtime resolve")
        .is_some());
}

#[test]
fn reconnect_within_window_rebinds_entity_a() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_with(runtime.clone());
    let _ = host.admit(
        "room-main".to_owned(),
        "c-bot01".to_owned(),
        credential(&keys, "Bot01", true),
    );
    let first = host.must_self("c-bot01");
    let entity_a = first.net_entity_id.clone();
    let first_session = first.session_id.clone();
    assert!(host
        .disconnect("c-bot01".to_owned())
        .expect("Runtime disconnect"));
    let rejected = host.admit_input_command("c-bot01".to_owned(), runtime_wire_chat_input());
    assert_eq!(rejected.kind, ChatOpKind::Rejected);
    let rebind = host.admit(
        "room-main".to_owned(),
        "c-bot01-re".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(rebind.reconnected);
    let rebound = host.must_self("c-bot01-re");
    assert_eq!(rebound.net_entity_id, entity_a);
    assert_ne!(rebound.session_id, first_session);
    assert_ne!(rebound.net_entity_id, rebound.session_id);

    // Rebinding within the retention window must cancel the old expiry. A
    // stale timer must never destroy the live rebound entity.
    host.clock().advance_ms(RECONNECT_WINDOW_MS + 1);
    assert!(host.drive_kernel());
    assert!(runtime.lock().expire_calls().is_empty());
}

#[test]
fn reconnect_missing_welcome_rearms_retained_entity_expiry() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_with(runtime.clone());
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-bot01".to_owned(),
            credential(&keys, "Bot01", true),
        )
        .accepted
    );
    let entity_a = host.must_self("c-bot01").net_entity_id;
    assert!(host.disconnect("c-bot01".to_owned()).expect("disconnect"));

    runtime.lock().suppress_rebind_welcome();
    let pending = host.admit(
        "room-main".to_owned(),
        "c-bot01-reconnected".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(pending.accepted && pending.reconnected);
    host.clock().advance_ms(RECONNECT_WINDOW_MS + 1);
    assert!(host.drive_kernel());
    assert!(runtime.lock().expire_calls().is_empty());

    assert!(host.run_tick("room-main".to_owned()).ok);
    assert!(host
        .try_self_lookup("c-bot01-reconnected".to_owned())
        .is_none());

    host.clock().advance_ms(RECONNECT_WINDOW_MS + 1);
    assert!(host.drive_kernel());
    assert!(runtime
        .lock()
        .expire_calls()
        .iter()
        .any(|id| id == &entity_a));
}

#[test]
fn reconnect_error_rearms_retained_entity_expiry() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_with(runtime.clone());
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-bot01".to_owned(),
            credential(&keys, "Bot01", true),
        )
        .accepted
    );
    let entity_a = host.must_self("c-bot01").net_entity_id;
    assert!(host.disconnect("c-bot01".to_owned()).expect("disconnect"));

    host.clock().advance_ms(RECONNECT_WINDOW_MS - 1);
    let rejected = host.admit(
        "room-other".to_owned(),
        "c-bot01-wrong-room".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(!rejected.accepted);
    assert_eq!(rejected.error_code.as_deref(), Some("cross_room_reference"));

    host.clock().advance_ms(2);
    assert!(host.drive_kernel());
    assert!(runtime.lock().expire_calls().is_empty());
    host.clock().advance_ms(RECONNECT_WINDOW_MS);
    assert!(host.drive_kernel());
    assert!(runtime
        .lock()
        .expire_calls()
        .iter()
        .any(|id| id == &entity_a));
}

#[test]
fn production_cadence_is_not_starved_and_drives_pending_expiry_rooms() {
    let runtime = SharedRuntime::new();
    runtime.lock().enable_async_queries();
    let clock = SharedClock::system();
    let keys = generate_keys();
    let host = Arc::new(EntityChatHost::new(
        RECONNECT_WINDOW_MS,
        clock.clone(),
        Box::new(runtime.clone()),
        Box::new(TestKernel::new()),
        ADMISSION_KEY_ID,
        keys.public.to_vec(),
        1_000,
    ));
    assert!(
        host.admit(
            "room-expiry-only".to_owned(),
            "c-expiry-only".to_owned(),
            credential(&keys, "ExpiryOnlyBot", true),
        )
        .accepted
    );
    assert!(host
        .disconnect("c-expiry-only".to_owned())
        .expect("disconnect"));
    clock.advance_ms(RECONNECT_WINDOW_MS + 1);

    let flood = host.clone();
    let worker = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(120);
        while std::time::Instant::now() < deadline {
            let _ = flood.try_self_lookup("missing".to_owned());
        }
    });
    worker.join().expect("owner flood worker");

    assert!(!runtime.lock().expire_calls().is_empty());
    assert_eq!(host.drain_runtime_queries().len(), 1);
}

#[test]
fn wall_clock_kernel_expire_tombstones_a_and_creates_b() {
    let clock = SharedClock::test();
    let runtime = SharedRuntime::new();
    let keys = generate_keys();
    let host = EntityChatHost::new(
        RECONNECT_WINDOW_MS,
        clock.clone(),
        Box::new(runtime.clone()),
        Box::new(TestKernel::new()),
        ADMISSION_KEY_ID,
        keys.public.to_vec(),
        1_000,
    );
    let _ = host.admit(
        "room-main".to_owned(),
        "c-bot01".to_owned(),
        credential(&keys, "Bot01", true),
    );
    let entity_a = host.must_self("c-bot01").net_entity_id;
    let account = host.must_self("c-bot01").account_id;
    assert!(host
        .disconnect("c-bot01".to_owned())
        .expect("Runtime disconnect"));
    clock.advance_ms(RECONNECT_WINDOW_MS + 1);
    assert!(host.drive_kernel());
    assert!(runtime
        .lock()
        .expire_calls()
        .iter()
        .any(|id| id == &entity_a));
    let created_b = host.admit(
        "room-main".to_owned(),
        "c-bot01-b".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(created_b.accepted);
    let entity_b = host.must_self("c-bot01-b").net_entity_id;
    assert_ne!(entity_b, entity_a);
    let tomb = host.query_attribute(AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ServerAuthoritative,
        room_id: "room-main".to_owned(),
        net_entity_id: entity_a,
        attribute_id: "EntityIdentity.entityType".to_owned(),
        connection_generation: None,
    });
    assert_eq!(tomb.outcome, AttributeQueryOutcome::Tombstoned);
    assert_eq!(host.must_self("c-bot01-b").account_id, account);
}

#[test]
fn async_expiry_is_pending_until_owner_tick_and_reports_runtime_error() {
    let clock = SharedClock::test();
    let runtime = SharedRuntime::new();
    runtime.lock().enable_async_queries();
    let keys = generate_keys();
    let host = EntityChatHost::new(
        RECONNECT_WINDOW_MS,
        clock.clone(),
        Box::new(runtime.clone()),
        Box::new(TestKernel::new()),
        ADMISSION_KEY_ID,
        keys.public.to_vec(),
        1_000,
    );
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-bot01".to_owned(),
            credential(&keys, "Bot01", true),
        )
        .accepted
    );
    assert!(host.disconnect("c-bot01".to_owned()).expect("disconnect"));
    clock.advance_ms(RECONNECT_WINDOW_MS + 1);
    assert!(host.drive_kernel());
    assert!(host.run_tick("room-main".to_owned()).ok);

    runtime.lock().fail_next_async_query("expire_failed");
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-bot02".to_owned(),
            credential(&keys, "Bot02", true),
        )
        .accepted
    );
    assert!(host.disconnect("c-bot02".to_owned()).expect("disconnect"));
    clock.advance_ms(RECONNECT_WINDOW_MS + 1);
    assert!(host.drive_kernel());
    let tick = host.run_tick("room-main".to_owned());
    assert!(!tick.ok);
    assert_eq!(tick.code.as_deref(), Some("expire_failed"));
}

#[test]
fn isolation_rejects_cross_room_query() {
    let (host, keys) = host_with(SharedRuntime::new());
    let _ = host.admit(
        "room-main".to_owned(),
        "c-browser".to_owned(),
        credential(&keys, "Browser01", false),
    );
    let _ = host.admit(
        "room-iso".to_owned(),
        "iso-a".to_owned(),
        credential(&keys, "IsoPlayerA", false),
    );
    let browser = host.must_self("c-browser");
    let cross = host.query_attribute(AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ServerAuthoritative,
        room_id: "room-iso".to_owned(),
        net_entity_id: browser.net_entity_id,
        attribute_id: "EntityIdentity.entityType".to_owned(),
        connection_generation: None,
    });
    assert_eq!(cross.error_code.as_deref(), Some("cross_room_reference"));
}

#[test]
fn runtime_resolve_and_query_bridge_failures_remain_explicit() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_with(runtime.clone());
    let _admitted = host.admit(
        "room-main".to_owned(),
        "c-browser".to_owned(),
        credential(&keys, "Browser01", false),
    );
    let net_entity_id = host.must_self("c-browser").net_entity_id;

    runtime.lock().fail_resolve("resolve_result_missing");
    let resolve_error = host
        .try_resolve_by_net_entity_id("room-main".to_owned(), net_entity_id.clone())
        .expect_err("Runtime resolve bridge error must not become None");
    assert_eq!(resolve_error, "resolve_result_missing");

    runtime.lock().fail_query("query_result_malformed");
    let query = host.query_attribute(AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ServerAuthoritative,
        room_id: "room-main".to_owned(),
        net_entity_id,
        attribute_id: "EntityIdentity.entityType".to_owned(),
        connection_generation: None,
    });
    assert_eq!(query.outcome, AttributeQueryOutcome::RequestError);
    assert_eq!(query.error_code.as_deref(), Some("query_result_malformed"));
}

#[test]
fn attribute_query_is_forwarded_to_runtime() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_with(runtime.clone());
    let _ = host.admit(
        "room-main".to_owned(),
        "c-browser".to_owned(),
        credential(&keys, "Browser01", false),
    );
    let binding = host.must_self("c-browser");
    runtime.lock().plant_query(
        "room-main",
        &binding.net_entity_id,
        "ChatComponent.lastMessageText",
        QueryResult::fail(AttributeQueryOutcome::Invisible),
    );
    runtime.lock().plant_query(
        "room-main",
        &binding.net_entity_id,
        "EntityIdentity.claimedMark",
        QueryResult::fail(AttributeQueryOutcome::Unauthorized),
    );
    runtime.lock().plant_query(
        "room-main",
        &binding.net_entity_id,
        "EntityIdentity.accountId",
        QueryResult::request_error("undeclared_attribute"),
    );
    let ok = host.query_attribute(AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ServerAuthoritative,
        room_id: "room-main".to_owned(),
        net_entity_id: binding.net_entity_id.clone(),
        attribute_id: "EntityIdentity.entityType".to_owned(),
        connection_generation: None,
    });
    let invisible = host.query_attribute(AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ClientReplica,
        room_id: "room-main".to_owned(),
        net_entity_id: binding.net_entity_id.clone(),
        attribute_id: "ChatComponent.lastMessageText".to_owned(),
        connection_generation: None,
    });
    let unauthorized = host.query_attribute(AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ClientReplica,
        room_id: "room-main".to_owned(),
        net_entity_id: binding.net_entity_id.clone(),
        attribute_id: "EntityIdentity.claimedMark".to_owned(),
        connection_generation: None,
    });
    let missing = host.query_attribute(AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ServerAuthoritative,
        room_id: "room-main".to_owned(),
        net_entity_id: "ffffffffffffffffffffffffffffffff".to_owned(),
        attribute_id: "EntityIdentity.entityType".to_owned(),
        connection_generation: None,
    });
    let stale = host.query_attribute(AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ServerAuthoritative,
        room_id: "room-main".to_owned(),
        net_entity_id: binding.net_entity_id,
        attribute_id: "EntityIdentity.entityType".to_owned(),
        connection_generation: Some(0),
    });
    let undeclared = host.query_attribute(AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ServerAuthoritative,
        room_id: "room-main".to_owned(),
        net_entity_id: host.must_self("c-browser").net_entity_id,
        attribute_id: "EntityIdentity.accountId".to_owned(),
        connection_generation: None,
    });
    assert_eq!(ok.outcome, AttributeQueryOutcome::Ok);
    assert_eq!(invisible.outcome, AttributeQueryOutcome::Invisible);
    assert_eq!(unauthorized.outcome, AttributeQueryOutcome::Unauthorized);
    assert_eq!(missing.outcome, AttributeQueryOutcome::NonExistent);
    assert_eq!(stale.outcome, AttributeQueryOutcome::StaleGeneration);
    assert_eq!(undeclared.outcome, AttributeQueryOutcome::RequestError);
    assert_eq!(
        undeclared.error_code.as_deref(),
        Some("undeclared_attribute")
    );
}

#[test]
fn async_runtime_query_is_pending_until_owner_tick_then_correlates_success() {
    let runtime = SharedRuntime::new();
    runtime.lock().enable_async_queries();
    let (host, keys) = host_with(runtime);
    let _ = host.admit(
        "room-main".to_owned(),
        "c-browser".to_owned(),
        credential(&keys, "Browser01", false),
    );
    let binding = host.must_self("c-browser");
    let request = AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ServerAuthoritative,
        room_id: "room-main".to_owned(),
        net_entity_id: binding.net_entity_id,
        attribute_id: "EntityIdentity.entityType".to_owned(),
        connection_generation: None,
    };
    let pending = host.query_attribute(request.clone());
    assert_eq!(
        pending.error_code.as_deref(),
        Some("runtime_query_pending"),
        "query must expose pending before an owner tick"
    );
    assert!(pending.request_id.is_some());
    let tick = host.run_tick("room-main".to_owned());
    assert!(tick.ok);
    let records = host.drain_runtime_queries();
    assert_eq!(records.len(), 1);
    assert!(records[0].request_id.starts_with("server-a2-attribute-"));
    let completed = host.query_attribute(request);
    assert_eq!(completed.outcome, AttributeQueryOutcome::Ok);
    assert_eq!(completed.value.as_deref(), Some("player"));
}

#[test]
fn async_runtime_query_error_is_correlated_after_owner_tick() {
    let runtime = SharedRuntime::new();
    runtime.lock().fail_next_async_query("query_failed");
    let (host, keys) = host_with(runtime);
    let _ = host.admit(
        "room-main".to_owned(),
        "c-browser".to_owned(),
        credential(&keys, "Browser01", false),
    );
    let binding = host.must_self("c-browser");
    let request = AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ServerAuthoritative,
        room_id: "room-main".to_owned(),
        net_entity_id: binding.net_entity_id,
        attribute_id: "EntityIdentity.entityType".to_owned(),
        connection_generation: None,
    };
    assert_eq!(
        host.query_attribute(request.clone()).error_code.as_deref(),
        Some("runtime_query_pending")
    );
    assert!(host.run_tick("room-main".to_owned()).ok);
    let error = host.query_attribute(request);
    assert_eq!(error.outcome, AttributeQueryOutcome::RequestError);
    assert_eq!(error.error_code.as_deref(), Some("query_failed"));
}

#[test]
fn unique_async_queries_are_bounded_and_capacity_releases_after_consume() {
    const EXPECTED_CAPACITY: usize = MAX_PENDING_QUERIES;
    let runtime = SharedRuntime::new();
    runtime.lock().enable_async_queries();
    let (host, keys) = host_with(runtime.clone());
    let _ = host.admit(
        "room-main".to_owned(),
        "c-query-capacity".to_owned(),
        credential(&keys, "QueryCapacityBot", true),
    );
    let binding = host.must_self("c-query-capacity");

    let requests: Vec<_> = (0..=EXPECTED_CAPACITY)
        .map(|index| AttributeQueryRequest {
            caller_scope: AttributeQueryScope::ServerAuthoritative,
            room_id: "room-main".to_owned(),
            net_entity_id: binding.net_entity_id.clone(),
            attribute_id: format!("EntityIdentity.attr-{index}"),
            connection_generation: None,
        })
        .collect();
    for request in requests.iter().take(EXPECTED_CAPACITY) {
        assert_eq!(
            host.query_attribute(request.clone()).error_code.as_deref(),
            Some("runtime_query_pending")
        );
    }
    assert_eq!(
        host.query_attribute(requests[EXPECTED_CAPACITY].clone())
            .error_code
            .as_deref(),
        Some("runtime_query_capacity")
    );
    assert_eq!(
        runtime.lock().query_calls().len(),
        EXPECTED_CAPACITY,
        "capacity rejection must happen before Runtime enqueue"
    );

    assert!(host.run_tick("room-main".to_owned()).ok);
    let completed = host.query_attribute(requests[0].clone());
    assert_eq!(completed.outcome, AttributeQueryOutcome::Ok);
    assert_eq!(
        host.query_attribute(AttributeQueryRequest {
            attribute_id: "EntityIdentity.reused".to_owned(),
            ..requests[EXPECTED_CAPACITY].clone()
        })
        .error_code
        .as_deref(),
        Some("runtime_query_pending"),
        "consuming one completion must release exactly one correlation slot"
    );
}

#[test]
fn unique_failed_async_queries_are_bounded_without_orphaning_pending_callers() {
    const EXPECTED_CAPACITY: usize = MAX_PENDING_QUERIES;
    let runtime = SharedRuntime::new();
    runtime.lock().enable_async_queries();
    let (host, keys) = host_with(runtime.clone());
    let _ = host.admit(
        "room-main".to_owned(),
        "c-failure-capacity".to_owned(),
        credential(&keys, "FailureCapacityBot", true),
    );
    let binding = host.must_self("c-failure-capacity");

    for index in 0..EXPECTED_CAPACITY {
        runtime.lock().suppress_next_async_query_result();
        let request = AttributeQueryRequest {
            caller_scope: AttributeQueryScope::ServerAuthoritative,
            room_id: "room-main".to_owned(),
            net_entity_id: binding.net_entity_id.clone(),
            attribute_id: format!("EntityIdentity.failure-{index}"),
            connection_generation: None,
        };
        assert_eq!(
            host.query_attribute(request).error_code.as_deref(),
            Some("runtime_query_pending")
        );
        assert!(!host.run_tick("room-main".to_owned()).ok);
    }

    let rejected = host.query_attribute(AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ServerAuthoritative,
        room_id: "room-main".to_owned(),
        net_entity_id: binding.net_entity_id.clone(),
        attribute_id: "EntityIdentity.failure-overflow".to_owned(),
        connection_generation: None,
    });
    assert_eq!(
        rejected.error_code.as_deref(),
        Some("runtime_query_capacity")
    );

    let first_failure = AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ServerAuthoritative,
        room_id: "room-main".to_owned(),
        net_entity_id: binding.net_entity_id.clone(),
        attribute_id: "EntityIdentity.failure-0".to_owned(),
        connection_generation: None,
    };
    assert_eq!(
        host.query_attribute(first_failure.clone())
            .error_code
            .as_deref(),
        Some("runtime_failure"),
        "failed correlation is a one-shot terminal result"
    );
    assert_eq!(
        host.query_attribute(first_failure).error_code.as_deref(),
        Some("runtime_query_pending"),
        "consuming a failed result must release its correlation slot"
    );
}

#[test]
fn expiry_retries_after_query_correlation_capacity_releases() {
    const EXPECTED_CAPACITY: usize = MAX_PENDING_QUERIES;
    let runtime = SharedRuntime::new();
    runtime.lock().enable_async_queries();
    let (host, keys) = host_with(runtime.clone());
    let clock = host.clock();
    let _ = host.admit(
        "room-main".to_owned(),
        "c-expiry-capacity".to_owned(),
        credential(&keys, "ExpiryCapacityBot", true),
    );
    let binding = host.must_self("c-expiry-capacity");
    let requests: Vec<_> = (0..EXPECTED_CAPACITY)
        .map(|index| AttributeQueryRequest {
            caller_scope: AttributeQueryScope::ServerAuthoritative,
            room_id: "room-main".to_owned(),
            net_entity_id: binding.net_entity_id.clone(),
            attribute_id: format!("EntityIdentity.expiry-capacity-{index}"),
            connection_generation: None,
        })
        .collect();
    for request in &requests {
        assert_eq!(
            host.query_attribute(request.clone()).error_code.as_deref(),
            Some("runtime_query_pending")
        );
    }

    assert!(host
        .disconnect("c-expiry-capacity".to_owned())
        .expect("disconnect"));
    clock.advance_ms(RECONNECT_WINDOW_MS + 1);
    assert!(
        !host.drive_kernel(),
        "expiry must report correlation pressure"
    );
    assert!(runtime.lock().expire_calls().is_empty());

    assert!(!host.run_tick("room-main".to_owned()).ok);
    let released = host.query_attribute(requests[0].clone());
    assert_eq!(released.outcome, AttributeQueryOutcome::Ok);

    clock.advance_ms(1);
    assert!(
        host.drive_kernel(),
        "retained expiry must retry after release"
    );
    assert!(runtime
        .lock()
        .expire_calls()
        .iter()
        .any(|id| id == &binding.net_entity_id));
    assert!(host.run_tick("room-main".to_owned()).ok);
    let tombstone_request = AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ServerAuthoritative,
        room_id: "room-main".to_owned(),
        net_entity_id: binding.net_entity_id,
        attribute_id: "EntityIdentity.entityType".to_owned(),
        connection_generation: None,
    };
    assert_eq!(
        host.query_attribute(tombstone_request.clone())
            .error_code
            .as_deref(),
        Some("runtime_query_pending")
    );
    assert!(host.run_tick("room-main".to_owned()).ok);
    let tombstone = host.query_attribute(tombstone_request);
    assert_eq!(tombstone.outcome, AttributeQueryOutcome::Tombstoned);
}

#[test]
fn restore_does_not_create_active_sessions() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_with(runtime.clone());
    let _ = host.admit(
        "room-main".to_owned(),
        "c-bot01".to_owned(),
        credential(&keys, "Bot01", true),
    );
    let snapshot = host
        .capture_persist_snapshot("room-main".to_owned())
        .expect("capture");
    host.restore_persist_snapshot("room-main".to_owned(), snapshot)
        .expect("restore");
    assert_eq!(runtime.lock().restore_calls(), 1);
    assert!(host.try_self_lookup("c-bot01".to_owned()).is_some());
}

#[test]
fn runtime_disconnect_failure_preserves_session_and_does_not_schedule_expiry() {
    let clock = SharedClock::test();
    let runtime = SharedRuntime::new();
    let keys = generate_keys();
    let host = EntityChatHost::new(
        RECONNECT_WINDOW_MS,
        clock.clone(),
        Box::new(runtime.clone()),
        Box::new(TestKernel::new()),
        ADMISSION_KEY_ID,
        keys.public.to_vec(),
        1_000,
    );
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-bot01".to_owned(),
            credential(&keys, "Bot01", true),
        )
        .accepted
    );
    runtime.lock().fail_disconnect("disconnect_failed");

    assert_eq!(
        host.disconnect("c-bot01".to_owned())
            .expect_err("Runtime disconnect error"),
        "disconnect_failed"
    );
    assert!(host.try_self_lookup("c-bot01".to_owned()).is_some());
    clock.advance_ms(RECONNECT_WINDOW_MS + 1);
    assert!(host.drive_kernel());
    assert!(runtime.lock().expire_calls().is_empty());
}

#[test]
fn expiry_timer_schedule_failure_is_returned_to_disconnect_caller() {
    let clock = SharedClock::test();
    let runtime = SharedRuntime::new();
    let keys = generate_keys();
    let mut kernel = TestKernel::new();
    kernel.fail_next_one_shot();
    let host = EntityChatHost::new(
        RECONNECT_WINDOW_MS,
        clock,
        Box::new(runtime),
        Box::new(kernel),
        ADMISSION_KEY_ID,
        keys.public.to_vec(),
        1_000,
    );
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-timer-failure".to_owned(),
            credential(&keys, "TimerFailureBot", true),
        )
        .accepted
    );

    let error = host
        .disconnect("c-timer-failure".to_owned())
        .expect_err("timer scheduling failure must be explicit");
    assert_eq!(error, "kernel_timer_schedule_failed");
}

#[test]
fn failed_tick_releases_pending_wire_batch_for_the_next_tick() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_with(runtime.clone());
    let (observer_tx, observer_rx) = bounded_channel(4);
    host.attach_wire_input_observer(observer_tx);
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-bot01".to_owned(),
            credential(&keys, "Bot01", true),
        )
        .accepted
    );
    let mut client =
        lumio_server_process::entity_chat::RoomClient::connect(&host.listen_uri(), "c-bot01")
            .expect("connect");
    let _ = client.recv_text();
    client
        .send_text(RUNTIME_WIRE_CHAT_INPUT)
        .expect("first input");
    observer_rx
        .recv_timeout(std::time::Duration::from_millis(500))
        .expect("Room wire input must be observed before ticking");
    runtime.lock().fail_next_tick("tick_failed");

    let failed = host.run_tick("room-main".to_owned());
    assert!(!failed.ok);

    client
        .send_text(RUNTIME_WIRE_CHAT_INPUT)
        .expect("second input");
    observer_rx
        .recv_timeout(std::time::Duration::from_millis(500))
        .expect("second Room wire input must be observed after failed tick");
    assert!(host.run_tick("room-main".to_owned()).ok);
}

#[test]
fn takeover_uses_runtime_addressed_connection_when_local_session_is_missing() {
    let runtime = SharedRuntime::new();
    runtime.lock().seed_live_binding(
        "runtime-old",
        "acct_Bot01",
        "room-main",
        BoundEntityKind::Bot,
    );
    let (host, keys) = host_with(runtime);

    let takeover = host.admit(
        "room-main".to_owned(),
        "c-new".to_owned(),
        credential(&keys, "Bot01", true),
    );

    assert!(takeover.accepted);
    assert!(takeover.takeover);
    assert!(host.try_self_lookup("c-new".to_owned()).is_some());
}

#[test]
fn takeover_rejection_does_not_stage_cross_room_pending_admission() {
    let runtime = SharedRuntime::new();
    runtime.lock().seed_live_binding(
        "runtime-old",
        "acct_Bot01",
        "room-other",
        BoundEntityKind::Bot,
    );
    let (host, keys) = host_with(runtime);
    let takeover = host.admit(
        "room-main".to_owned(),
        "c-new".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(!takeover.accepted);
    assert_eq!(takeover.error_code.as_deref(), Some("cross_room_reference"));
    assert!(
        !host.disconnect("c-new".to_owned()).expect("disconnect"),
        "rejected takeover must not leave a pending admission"
    );
}

#[test]
fn takeover_pending_without_next_tick_identity_is_retired() {
    let runtime = SharedRuntime::new();
    runtime.lock().seed_live_binding(
        "runtime-old",
        "acct_Bot01",
        "room-main",
        BoundEntityKind::Bot,
    );
    runtime.lock().suppress_rebind_frames();
    let (host, keys) = host_with(runtime);
    let takeover = host.admit(
        "room-main".to_owned(),
        "c-new".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(takeover.accepted && takeover.takeover);
    assert!(host.try_self_lookup("c-new".to_owned()).is_none());
    assert!(host.run_tick("room-main".to_owned()).ok);
    assert!(host.try_self_lookup("c-new".to_owned()).is_none());
    assert!(
        !host.disconnect("c-new".to_owned()).expect("disconnect"),
        "no-Welcome retirement must clear pending admission state"
    );
}

#[test]
fn persist_and_restore_failures_are_returned_to_the_caller() {
    let runtime = SharedRuntime::new();
    let (host, _) = host_with(runtime.clone());
    runtime.lock().fail_persist("snapshot_failed");
    assert_eq!(
        host.capture_persist_snapshot("room-main".to_owned())
            .expect_err("capture error"),
        "snapshot_failed"
    );

    runtime.lock().fail_restore("restore_failed");
    assert_eq!(
        host.restore_persist_snapshot(
            "room-main".to_owned(),
            lumio_server_process::entity_chat::PersistRecord {
                bytes: b"persist".to_vec(),
            },
        )
        .expect_err("restore error"),
        "restore_failed"
    );
}

#[test]
fn malformed_runtime_frame_fails_closed_without_lossy_text() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_with(runtime.clone());
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-invalid".to_owned(),
            credential(&keys, "Bot01", true),
        )
        .accepted
    );
    runtime.lock().plant_raw_delta(vec![vec![0xff]]);

    assert!(!host.run_tick("room-main".to_owned()).ok);
    assert!(host.try_self_lookup("c-invalid".to_owned()).is_none());
}

#[test]
fn oversized_runtime_frame_fails_closed() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_with(runtime.clone());
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-oversized".to_owned(),
            credential(&keys, "Bot01", true),
        )
        .accepted
    );
    runtime.lock().plant_raw_delta(vec![vec![b'a'; 65_537]]);

    assert!(!host.run_tick("room-main".to_owned()).ok);
    assert!(host.try_self_lookup("c-oversized".to_owned()).is_none());
}

#[test]
fn kernel_tick_frame_runs_runtime_tick() {
    let (host, keys) = host_with(SharedRuntime::new());
    let _ = host.admit(
        "room-main".to_owned(),
        "c-bot01".to_owned(),
        credential(&keys, "Bot01", true),
    );
    let _ = host.admit_input_command("c-bot01".to_owned(), runtime_wire_chat_input());
    let tick = host.schedule_room_tick("room-main".to_owned(), 0);
    assert_eq!(tick.applied_tick, 1);
}

#[test]
fn batched_chat_inputs_stay_within_runtime_change_entry_budget() {
    assert_eq!(
        lumio_server_process::entity_chat::MAX_CHAT_INPUTS_PER_TICK * 2,
        128,
        "two ChatComponent field writes per chat.input must fit MaxChangeEntries=128"
    );
}

fn admit_n(
    host: &EntityChatHost,
    keys: &lumio_server_process::entity_chat::Ed25519KeyPair,
    n: usize,
) {
    for i in 1..=n {
        let name = format!("Bot{i:02}");
        let accepted = host
            .admit(
                "room-main".to_owned(),
                format!("c-{i:03}"),
                credential(keys, &name, true),
            )
            .accepted;
        assert!(accepted, "admit {name}");
    }
}

fn enqueue_n(host: &EntityChatHost, n: usize) {
    for i in 1..=n {
        let admitted = host.admit_input_command(format!("c-{i:03}"), runtime_wire_chat_input());
        assert_eq!(admitted.kind, ChatOpKind::Admitted);
    }
}

#[test]
fn sixty_four_chat_inputs_one_tick_emit_chat_event() {
    let (host, keys) = host_with(SharedRuntime::new());
    admit_n(&host, &keys, 64);
    let mut client =
        lumio_server_process::entity_chat::RoomClient::connect(&host.listen_uri(), "c-001")
            .expect("connect");
    let _ = client.recv_text();
    enqueue_n(&host, 64);
    let tick = host.run_tick("room-main".to_owned());
    assert!(tick.ok, "N=64 must succeed, got {tick:?}");
    assert_eq!(tick.event_count, 64);
    let frame = client.recv_text().expect("delta");
    assert!(
        frame.contains("\"messageType\":\"WorldChange\"")
            && frame.contains("\"method\":\"OnChatMessage\""),
        "N=64 must emit Runtime ChatComponent.OnChatMessage, got {frame}"
    );
}

#[test]
fn host_wire_ingress_ticks_at_max_chat_inputs() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_with(runtime.clone());
    let _ = host.admit(
        "room-main".to_owned(),
        "c-bot01".to_owned(),
        credential(&keys, "Bot01", true),
    );
    let mut client =
        lumio_server_process::entity_chat::RoomClient::connect(&host.listen_uri(), "c-bot01")
            .expect("connect");
    let _ = client.recv_text();
    for _ in 0..=MAX_CHAT_INPUTS_PER_TICK {
        client
            .send_text(RUNTIME_WIRE_CHAT_INPUT)
            .expect("wire chat.input");
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while runtime.lock().run_tick_input_counts().is_empty() && std::time::Instant::now() < deadline
    {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let counts = runtime.lock().run_tick_input_counts().to_vec();
    assert_eq!(counts.first().copied(), Some(MAX_CHAT_INPUTS_PER_TICK));
    assert!(
        counts.iter().all(|n| *n <= MAX_CHAT_INPUTS_PER_TICK),
        "host must not forward more than {MAX_CHAT_INPUTS_PER_TICK} chat.inputs to Runtime RunTick, got {counts:?}"
    );
}

#[test]
fn wire_ingress_bounds_each_connection_before_a_shared_tick() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_with(runtime.clone());
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-a".to_owned(),
            credential(&keys, "Bot01", true),
        )
        .accepted
    );
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-b".to_owned(),
            credential(&keys, "Bot02", true),
        )
        .accepted
    );
    let mut a = lumio_server_process::entity_chat::RoomClient::connect(&host.listen_uri(), "c-a")
        .expect("connect a");
    let mut b = lumio_server_process::entity_chat::RoomClient::connect(&host.listen_uri(), "c-b")
        .expect("connect b");
    let _ = a.recv_text();
    let _ = b.recv_text();

    // Keep one other connection pending so c-a cannot trigger its automatic
    // single-connection batch tick while the bound is exercised.
    b.send_text(RUNTIME_WIRE_CHAT_INPUT).expect("input b");
    std::thread::sleep(std::time::Duration::from_millis(80));
    for _ in 0..(MAX_CHAT_INPUTS_PER_TICK) {
        a.send_text(RUNTIME_WIRE_CHAT_INPUT).expect("input a");
    }
    a.send_text(RUNTIME_WIRE_CHAT_INPUT)
        .expect("overflow input a");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while host.try_self_lookup("c-a".to_owned()).is_some() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(
        host.try_self_lookup("c-a".to_owned()).is_none(),
        "overflow must retire only the noisy connection"
    );
    assert!(runtime
        .lock()
        .disconnect_calls()
        .iter()
        .any(|connection| connection == "c-a"));
}

#[test]
fn deferred_overflow_retires_session_and_disconnects_runtime_binding() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_with(runtime.clone());
    let admitted = host.admit(
        "room-main".to_owned(),
        "c-overflow".to_owned(),
        credential(&keys, "OverflowBot", true),
    );
    assert!(admitted.accepted);

    // No socket is attached, so each Runtime frame is deferred until the
    // per-connection bound is reached and the logical session is retired.
    runtime.lock().plant_delta(vec!["deferred".to_owned()]);
    for _ in 0..=MAX_DEFERRED_FRAMES_PER_CONNECTION {
        let _ = host.run_tick("room-main".to_owned());
    }

    assert!(
        host.try_self_lookup("c-overflow".to_owned()).is_none(),
        "overflow must remove the logical session"
    );
    assert!(
        !host
            .disconnect("c-overflow".to_owned())
            .expect("disconnect"),
        "overflow retirement must clear the host connection state"
    );
    assert!(
        runtime
            .lock()
            .disconnect_calls()
            .iter()
            .any(|connection| connection == "c-overflow"),
        "overflow must ask Runtime to disconnect the retired binding"
    );
    let reconnected = host.admit(
        "room-main".to_owned(),
        "c-overflow-reconnected".to_owned(),
        credential(&keys, "OverflowBot", true),
    );
    assert!(
        reconnected.reconnected,
        "retired overflow session must not block account reconnect"
    );
}

#[test]
fn sixty_five_chat_inputs_one_tick_empty_delta_is_not_success() {
    let (host, keys) = host_with(SharedRuntime::new());
    admit_n(&host, &keys, 65);
    let mut client =
        lumio_server_process::entity_chat::RoomClient::connect(&host.listen_uri(), "c-001")
            .expect("connect");
    let _ = client.recv_text();
    enqueue_n(&host, 65);
    let tick = host.run_tick("room-main".to_owned());
    assert!(
        !tick.ok,
        "N=65 must not be SUCCESS (Runtime MaxChangeEntries=128), got {tick:?}"
    );
    assert_eq!(tick.event_count, 0);
    match client.recv_text() {
        Ok(frame) => {
            assert!(
                !frame.contains("\"method\":\"OnChatMessage\""),
                "N=65 must not emit Runtime chat RPC, got {frame}"
            );
            assert!(!tick.ok, "N=65 WorldChange is not SUCCESS, got {frame}");
        }
        Err(_) => assert!(
            !tick.ok,
            "budget fault must not be treated as a successful tick"
        ),
    }
}

#[test]
fn oversized_direct_input_is_rejected_before_runtime() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_with(runtime.clone());
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-oversized-input".to_owned(),
            credential(&keys, "Bot01", true),
        )
        .accepted
    );
    let result = host.admit_input_command(
        "c-oversized-input".to_owned(),
        vec![b'a'; lumio_server_process::entity_chat::MAX_WIRE_TEXT_BYTES + 1],
    );
    assert_eq!(result.kind, ChatOpKind::Rejected);
    assert_eq!(result.error_code.as_deref(), Some("bad_envelope"));
    assert!(runtime.lock().run_tick_input_counts().is_empty());
}

#[test]
fn async_admission_is_pending_until_owner_tick_welcome() {
    let runtime = SharedRuntime::new();
    runtime.lock().enable_async_admissions();
    let (host, keys) = host_with(runtime);
    let admitted = host.admit(
        "room-main".to_owned(),
        "c-delayed".to_owned(),
        credential(&keys, "DelayedBot", true),
    );
    assert!(admitted.accepted);
    assert!(host.try_self_lookup("c-delayed".to_owned()).is_none());
    assert!(host.run_tick("room-main".to_owned()).ok);
    assert!(host.try_self_lookup("c-delayed".to_owned()).is_some());
}

#[test]
fn duplicate_pending_admission_is_rejected_before_runtime_enqueue() {
    let runtime = SharedRuntime::new();
    runtime.lock().enable_async_admissions();
    let (host, keys) = host_with(runtime.clone());
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-duplicate".to_owned(),
            credential(&keys, "DuplicateBot", true),
        )
        .accepted
    );
    let duplicate = host.admit(
        "room-main".to_owned(),
        "c-duplicate".to_owned(),
        credential(&keys, "DuplicateBot", true),
    );
    assert!(!duplicate.accepted);
    assert_eq!(duplicate.error_code.as_deref(), Some("admission_pending"));
    assert_eq!(runtime.lock().admit_calls().len(), 1);
}

#[test]
fn active_account_selects_takeover_before_async_runtime_admission() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_with(runtime.clone());
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-old".to_owned(),
            credential(&keys, "ActiveTakeoverBot", true),
        )
        .accepted
    );

    let takeover = host.admit(
        "room-main".to_owned(),
        "c-new".to_owned(),
        credential(&keys, "ActiveTakeoverBot", true),
    );

    assert!(takeover.accepted && takeover.takeover);
    assert!(host.try_self_lookup("c-new".to_owned()).is_some());
    assert!(host.try_self_lookup("c-old".to_owned()).is_none());
    assert_eq!(runtime.lock().admit_calls().len(), 1);
}

#[test]
fn takeover_cleans_a_superseded_pending_admission() {
    let runtime = SharedRuntime::new();
    runtime.lock().enable_async_admissions();
    let (host, keys) = host_with(runtime);
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-pending-old".to_owned(),
            credential(&keys, "PendingTakeoverBot", true),
        )
        .accepted
    );
    let takeover = host.admit(
        "room-main".to_owned(),
        "c-takeover".to_owned(),
        credential(&keys, "PendingTakeoverBot", true),
    );
    assert!(takeover.accepted && takeover.takeover);
    assert!(host.try_self_lookup("c-pending-old".to_owned()).is_none());
    assert!(host.try_self_lookup("c-takeover".to_owned()).is_some());
}

#[test]
fn missing_async_query_result_fails_and_releases_correlation() {
    let runtime = SharedRuntime::new();
    runtime.lock().enable_async_queries();
    let (host, keys) = host_with(runtime.clone());
    let _ = host.admit(
        "room-main".to_owned(),
        "c-missing-query".to_owned(),
        credential(&keys, "MissingQueryBot", true),
    );
    let binding = host.must_self("c-missing-query");
    let request = AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ServerAuthoritative,
        room_id: "room-main".to_owned(),
        net_entity_id: binding.net_entity_id,
        attribute_id: "EntityIdentity.entityType".to_owned(),
        connection_generation: None,
    };
    runtime.lock().suppress_next_async_query_result();
    assert_eq!(
        host.query_attribute(request.clone()).error_code.as_deref(),
        Some("runtime_query_pending")
    );
    let tick = host.run_tick("room-main".to_owned());
    assert!(!tick.ok);
    let failed = host.query_attribute(request);
    assert_eq!(failed.outcome, AttributeQueryOutcome::RequestError);
    assert_eq!(failed.error_code.as_deref(), Some("runtime_failure"));
}

#[test]
fn malformed_async_query_result_fails_and_releases_correlation() {
    let runtime = SharedRuntime::new();
    runtime.lock().enable_async_queries();
    let (host, keys) = host_with(runtime.clone());
    let _ = host.admit(
        "room-main".to_owned(),
        "c-malformed-query".to_owned(),
        credential(&keys, "MalformedQueryBot", true),
    );
    let binding = host.must_self("c-malformed-query");
    let request = AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ServerAuthoritative,
        room_id: "room-main".to_owned(),
        net_entity_id: binding.net_entity_id,
        attribute_id: "EntityIdentity.entityType".to_owned(),
        connection_generation: None,
    };
    runtime.lock().malform_next_async_query_result();
    assert_eq!(
        host.query_attribute(request.clone()).error_code.as_deref(),
        Some("runtime_query_pending")
    );
    let tick = host.run_tick("room-main".to_owned());
    assert!(!tick.ok);
    let failed = host.query_attribute(request);
    assert_eq!(failed.outcome, AttributeQueryOutcome::RequestError);
    assert_eq!(failed.error_code.as_deref(), Some("runtime_failure"));
}

#[test]
fn pending_disconnect_enqueues_runtime_intent_and_clears_state() {
    let runtime = SharedRuntime::new();
    runtime.lock().enable_async_admissions();
    let (host, keys) = host_with(runtime.clone());
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-pending-disconnect".to_owned(),
            credential(&keys, "PendingDisconnectBot", true),
        )
        .accepted
    );
    assert!(host
        .disconnect("c-pending-disconnect".to_owned())
        .expect("disconnect"));
    assert!(host
        .try_self_lookup("c-pending-disconnect".to_owned())
        .is_none());
    assert!(
        !host
            .disconnect("c-pending-disconnect".to_owned())
            .expect("second disconnect"),
        "pending disconnect must clear all host admission state"
    );
    assert!(runtime
        .lock()
        .disconnect_calls()
        .iter()
        .any(|connection| connection == "c-pending-disconnect"));
    assert!(host.run_tick("room-main".to_owned()).ok);
    assert!(host
        .try_self_lookup("c-pending-disconnect".to_owned())
        .is_none());
}

#[test]
fn pending_admission_capacity_rejects_at_boundary_and_reuses_after_disconnect() {
    let runtime = SharedRuntime::new();
    runtime.lock().enable_async_admissions();
    let (host, keys) = host_with(runtime.clone());
    for i in 0..MAX_PENDING_ADMISSIONS {
        let result = host.admit(
            "room-main".to_owned(),
            format!("c-capacity-{i}"),
            credential(&keys, &format!("CapacityBot{i}"), true),
        );
        assert!(result.accepted, "pending admission {i} must fit");
    }
    let rejected = host.admit(
        "room-main".to_owned(),
        "c-capacity-over".to_owned(),
        credential(&keys, "CapacityOverflowBot", true),
    );
    assert!(!rejected.accepted);
    assert_eq!(rejected.error_code.as_deref(), Some("admission_capacity"));
    assert_eq!(runtime.lock().admit_calls().len(), MAX_PENDING_ADMISSIONS);
    assert!(host
        .disconnect("c-capacity-0".to_owned())
        .expect("disconnect"));
    let reused = host.admit(
        "room-main".to_owned(),
        "c-capacity-reused".to_owned(),
        credential(&keys, "CapacityReusedBot", true),
    );
    assert!(reused.accepted);
}

#[test]
fn repeated_expiry_results_release_runtime_correlations() {
    let runtime = SharedRuntime::new();
    runtime.lock().enable_async_queries();
    let clock = SharedClock::test();
    let keys = generate_keys();
    let host = EntityChatHost::new(
        RECONNECT_WINDOW_MS,
        clock.clone(),
        Box::new(runtime.clone()),
        Box::new(TestKernel::new()),
        ADMISSION_KEY_ID,
        keys.public.to_vec(),
        1_000,
    );
    for i in 0..8 {
        let connection = format!("c-expiry-{i}");
        let name = format!("ExpiryBot{i}");
        assert!(
            host.admit(
                "room-main".to_owned(),
                connection.clone(),
                credential(&keys, &name, true)
            )
            .accepted
        );
        assert!(host.disconnect(connection).expect("disconnect"));
        clock.advance_ms(RECONNECT_WINDOW_MS + 1);
        assert!(host.drive_kernel());
        assert!(host.run_tick("room-main".to_owned()).ok);
        assert_eq!(host.drain_runtime_queries().len(), 1);
    }
}

#[test]
fn claimed_mark_client_replica_is_contract_unauthorized() {
    let (host, keys) = host_with(SharedRuntime::new());
    let _ = host.admit(
        "room-main".to_owned(),
        "c-browser".to_owned(),
        credential(&keys, "Browser01", false),
    );
    let binding = host.must_self("c-browser");
    let unauthorized = host.query_attribute(AttributeQueryRequest {
        caller_scope: AttributeQueryScope::ClientReplica,
        room_id: "room-main".to_owned(),
        net_entity_id: binding.net_entity_id,
        attribute_id: "EntityIdentity.claimedMark".to_owned(),
        connection_generation: None,
    });
    assert_eq!(
        unauthorized.outcome,
        AttributeQueryOutcome::Unauthorized,
        "claim-scoped claimedMark without a claim is contract Unauthorized, not {:?}",
        unauthorized.outcome
    );
}

#[test]
fn resolve_requires_canonical_runtime_id() {
    let (host, keys) = host_with(SharedRuntime::new());
    let _bot = host.admit(
        "room-main".to_owned(),
        "c-bot01".to_owned(),
        credential(&keys, "Bot01", true),
    );
    let hex = host.must_self("c-bot01").net_entity_id;
    assert_eq!(hex.len(), 32);
    assert!(host
        .try_resolve_by_net_entity_id("room-main".to_owned(), hex.clone())
        .expect("Runtime resolve")
        .is_some());
    let as_u64 = u64::from_str_radix(&hex, 16).expect("runtime 32-hex is a u64");
    let resolved = host
        .try_resolve_by_net_entity_id("room-main".to_owned(), as_u64.to_string())
        .expect("Runtime resolve");
    assert!(
        resolved.is_none(),
        "non-canonical Runtime ID {as_u64} must not be normalized by the Server host"
    );
}
