//! Room wire: R5 Runtime messages from the consume-only Runtime test double.

mod common;

use common::{
    runtime_wire_chat_input, welcome_frame, world_change_frame, SharedRuntime, TestKernel,
    RUNTIME_WIRE_CHAT_INPUT,
};
use lumio_host_runtime::{bounded_channel, SharedClock};
use lumio_server_process::entity_chat::{
    drain_chat_event_deltas, generate_keys, issue_admission_credential, ChatOpKind, EntityChatHost,
    RoomClient, ADMISSION_KEY_ID, MAX_CHAT_INPUTS_PER_TICK, MAX_WIRE_TEXT_BYTES,
    RECONNECT_WINDOW_MS,
};

fn credential(
    keys: &lumio_server_process::entity_chat::Ed25519KeyPair,
    name: &str,
    bot: bool,
) -> String {
    issue_admission_credential(&keys.seed, 1, &format!("acct_{name}"), name, bot, 1, 9_000)
}

fn host_ready(
    runtime: SharedRuntime,
) -> (
    EntityChatHost,
    lumio_server_process::entity_chat::Ed25519KeyPair,
) {
    let keys = generate_keys();
    runtime.lock().plant_snapshot(&welcome_frame());
    runtime
        .lock()
        .plant_delta(vec![world_change_frame(1), world_change_frame(2)]);
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

#[test]
fn runtime_welcome_contains_canonical_self_identity() {
    let (host, keys) = host_ready(SharedRuntime::new());
    let admit = host.admit(
        "room-main".to_owned(),
        "c-bot01".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(admit.accepted);
    let mut client = RoomClient::connect(&host.listen_uri(), "c-bot01").expect("connect");
    let frame = client.recv_text().expect("snapshot");
    assert!(frame.contains("\"messageType\":\"Welcome\""));
    assert!(frame.contains("\"selfNetEntityId\""));
}

#[test]
fn room_client_chat_input_over_wire_then_tick_sends_chat_event_delta() {
    let keys = generate_keys();
    let host = EntityChatHost::new(
        RECONNECT_WINDOW_MS,
        SharedClock::test(),
        Box::new(SharedRuntime::new()),
        Box::new(TestKernel::new()),
        ADMISSION_KEY_ID,
        keys.public.to_vec(),
        1_000,
    );
    let admit = host.admit(
        "room-main".to_owned(),
        "c-bot01".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(admit.accepted);
    let mut client = RoomClient::connect(&host.listen_uri(), "c-bot01").expect("connect");
    let _ = client.recv_text();
    client
        .send_text(RUNTIME_WIRE_CHAT_INPUT)
        .expect("wire chat.input");
    std::thread::sleep(std::time::Duration::from_millis(80));
    let tick = host.run_tick("room-main".to_owned());
    assert!(tick.ok, "kernel tickFrame must run, got {tick:?}");
    let frame = client.recv_text().expect("delta");
    assert!(
        frame.contains("\"messageType\":\"WorldChange\""),
        "Room WS chat.input must become Runtime WorldChange, got {frame}"
    );
}

#[test]
fn wire_input_observer_confirms_room_ingress_before_tick() {
    let keys = generate_keys();
    let (observer_tx, observer_rx) = bounded_channel(4);
    let host = EntityChatHost::new(
        RECONNECT_WINDOW_MS,
        SharedClock::test(),
        Box::new(SharedRuntime::new()),
        Box::new(TestKernel::new()),
        ADMISSION_KEY_ID,
        keys.public.to_vec(),
        1_000,
    );
    host.attach_wire_input_observer(observer_tx);
    let admit = host.admit(
        "room-main".to_owned(),
        "c-bot01".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(admit.accepted);
    let mut client = RoomClient::connect(&host.listen_uri(), "c-bot01").expect("connect");
    let _ = client.recv_text();
    client
        .send_text(RUNTIME_WIRE_CHAT_INPUT)
        .expect("wire chat.input");
    observer_rx
        .recv_timeout(std::time::Duration::from_millis(500))
        .expect("Room WS chat.input must be observed before tick");
    let tick = host.run_tick("room-main".to_owned());
    assert!(tick.ok, "kernel tickFrame must run, got {tick:?}");
}

#[test]
fn admitted_wire_input_is_observed_byte_for_byte_without_host_retention() {
    let keys = generate_keys();
    let (observer_tx, captured_rx) = bounded_channel::<Vec<u8>>(1);
    let host = EntityChatHost::new(
        RECONNECT_WINDOW_MS,
        SharedClock::test(),
        Box::new(SharedRuntime::new()),
        Box::new(TestKernel::new()),
        ADMISSION_KEY_ID,
        keys.public.to_vec(),
        1_000,
    );
    host.attach_wire_input_observer(observer_tx);
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-bot01".to_owned(),
            credential(&keys, "Bot01", true),
        )
        .accepted
    );
    let mut client = RoomClient::connect(&host.listen_uri(), "c-bot01").expect("connect");
    let _ = client.recv_text();
    let input = runtime_wire_chat_input();
    client
        .send_text(std::str::from_utf8(&input).expect("utf8 fixture"))
        .expect("wire input");
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
    let mut captured = None;
    while captured.is_none() && std::time::Instant::now() < deadline {
        captured = captured_rx.try_recv().ok();
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(captured.as_deref(), Some(input.as_slice()));
}

#[test]
fn admit_chat_input_then_tick_sends_chat_event_delta_to_room_client() {
    let keys = generate_keys();
    let host = EntityChatHost::new(
        RECONNECT_WINDOW_MS,
        SharedClock::test(),
        Box::new(SharedRuntime::new()),
        Box::new(TestKernel::new()),
        ADMISSION_KEY_ID,
        keys.public.to_vec(),
        1_000,
    );
    let admit = host.admit(
        "room-main".to_owned(),
        "c-bot01".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(admit.accepted);
    let mut client = RoomClient::connect(&host.listen_uri(), "c-bot01").expect("connect");
    let _ = client.recv_text();
    let admitted = host.admit_input_command("c-bot01".to_owned(), runtime_wire_chat_input());
    assert_eq!(admitted.kind, ChatOpKind::Admitted);
    let tick = host.run_tick("room-main".to_owned());
    assert!(
        tick.applied_tick >= 1,
        "kernel tickFrame must run, got {tick:?}"
    );
    let frame = client.recv_text().expect("delta");
    assert!(
        frame.contains("\"messageType\":\"WorldChange\""),
        "Room client must receive a C-1 WorldChange, got {frame}"
    );
    assert!(
        frame.contains("\"method\":\"OnChatMessage\""),
        "WorldChange.rpcs must contain Runtime ChatComponent.OnChatMessage, got {frame}"
    );
    assert!(
        !frame.contains("\"rpcs\":[]"),
        "live-equivalent room input + tick must not broadcast empty WorldChange.rpcs, got {frame}"
    );
}

#[test]
fn tick_broadcasts_runtime_delta_bytes_in_order() {
    let (host, keys) = host_ready(SharedRuntime::new());
    let _ = host.admit(
        "room-main".to_owned(),
        "c-a".to_owned(),
        credential(&keys, "Bot01", true),
    );
    let _ = host.admit(
        "room-main".to_owned(),
        "c-b".to_owned(),
        credential(&keys, "Bot02", true),
    );
    let mut a = RoomClient::connect(&host.listen_uri(), "c-a").expect("a");
    let mut b = RoomClient::connect(&host.listen_uri(), "c-b").expect("b");
    let _ = a.recv_text();
    let _ = b.recv_text();
    let _ = host.admit_input_command("c-a".to_owned(), runtime_wire_chat_input());
    let tick = host.run_tick("room-main".to_owned());
    assert_eq!(tick.applied_tick, 1);
    let first_a = a.recv_text().expect("a1");
    let second_a = a.recv_text().expect("a2");
    let first_b = b.recv_text().expect("b1");
    let second_b = b.recv_text().expect("b2");
    assert!(first_a.contains("\"messageType\":\"WorldChange\""));
    assert!(second_a.contains("\"messageType\":\"WorldChange\""));
    assert_eq!(first_a, first_b);
    assert_eq!(second_a, second_b);
    assert_ne!(first_a, second_a);
}

#[test]
fn takeover_sends_connection_superseded_before_close() {
    let (host, keys) = host_ready(SharedRuntime::new());
    let first = host.admit(
        "room-main".to_owned(),
        "c-old".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(first.accepted);
    let mut old = RoomClient::connect(&host.listen_uri(), "c-old").expect("old");
    let _ = old.recv_text();
    let takeover = host.admit(
        "room-main".to_owned(),
        "c-new".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(takeover.takeover);
    let notice = old.recv_text().expect("superseded");
    assert!(
        notice.contains("\"messageType\":\"ConnectionSuperseded\""),
        "old client must receive ConnectionSuperseded first, got {notice}"
    );
    assert!(notice.contains("\"reasonCode\":\"connection_superseded\""));
    assert!(
        old.is_closed_after(),
        "old socket must close after ConnectionSuperseded"
    );
    let mut new_client = RoomClient::connect(&host.listen_uri(), "c-new").expect("new");
    let snapshot = new_client.recv_text().expect("new snapshot");
    assert!(snapshot.contains("\"messageType\":\"Welcome\""));
    assert!(snapshot.contains("\"selfNetEntityId\""));
}

#[test]
fn takeover_pending_tick_without_welcome_keeps_superseded_frame_and_cleans_new_socket() {
    let runtime = SharedRuntime::new();
    runtime.lock().seed_live_binding(
        "runtime-old",
        "acct_Bot01",
        "room-main",
        lumio_server_process::entity_chat::BoundEntityKind::Bot,
    );
    runtime.lock().suppress_rebind_welcome();
    let (host, keys) = host_ready(runtime.clone());
    runtime.lock().plant_raw_delta(Vec::new());
    let mut old = RoomClient::connect(&host.listen_uri(), "runtime-old").expect("old connect");
    let mut new = RoomClient::connect(&host.listen_uri(), "c-new").expect("new connect");
    let takeover = host.admit(
        "room-main".to_owned(),
        "c-new".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(takeover.accepted && takeover.takeover);
    let superseded = old.recv_text().expect("superseded frame must be routed");
    assert!(superseded.contains("\"messageType\":\"ConnectionSuperseded\""));
    assert!(old.is_closed_after());
    assert!(host.run_tick("room-main".to_owned()).ok);
    assert!(host.try_self_lookup("c-new".to_owned()).is_none());
    assert!(new.is_closed_after());
}

const HOST_MINTED_EMPTY: &str = r#"{"connectionGeneration":1,"instanceId":0,"messageType":"Welcome","selfNetEntityId":"00000000000000000000000000000001"}"#;

#[test]
fn runtime_snapshot_failure_does_not_send_host_minted_empty_full_snapshot() {
    let runtime = SharedRuntime::new();
    runtime.lock().fail_snapshot();
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
    let admit = host.admit(
        "room-main".to_owned(),
        "c-bot01".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(admit.accepted);
    let mut client = RoomClient::connect(&host.listen_uri(), "c-bot01").expect("connect");
    let frame = client.recv_text();
    assert!(
        frame.as_ref().is_err() || frame.as_ref().is_ok_and(|text| text != HOST_MINTED_EMPTY),
        "client must not receive a host-minted empty Welcome, got {frame:?}"
    );
    assert!(
        client.received.iter().all(|text| text != HOST_MINTED_EMPTY),
        "no host-invented empty Welcome on the wire, got {:?}",
        client.received
    );
}

#[test]
fn pre_admission_socket_receives_runtime_rejection_frame() {
    let runtime = SharedRuntime::new();
    runtime
        .lock()
        .reject_next_admit_with_frame("invalid_binding_shape");
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
    let mut client = RoomClient::connect(&host.listen_uri(), "c-rejected").expect("connect");
    let result = host.admit(
        "room-main".to_owned(),
        "c-rejected".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(!result.accepted);
    let frame = client.recv_text().expect("runtime rejection frame");
    assert!(frame.contains("\"messageType\":\"Error\""), "got {frame}");
}

fn send_n_wire_chats(client: &mut RoomClient, n: usize) {
    for _ in 0..n {
        client
            .send_text(RUNTIME_WIRE_CHAT_INPUT)
            .expect("wire chat.input");
    }
}

#[test]
fn drain_chat_event_deltas_returns_before_deadline_when_idle() {
    let keys = generate_keys();
    let host = EntityChatHost::new(
        RECONNECT_WINDOW_MS,
        SharedClock::test(),
        Box::new(SharedRuntime::new()),
        Box::new(TestKernel::new()),
        ADMISSION_KEY_ID,
        keys.public.to_vec(),
        1_000,
    );
    let admit = host.admit(
        "room-main".to_owned(),
        "c-browser".to_owned(),
        credential(&keys, "Browser01", false),
    );
    assert!(admit.accepted);
    let mut client = RoomClient::connect(&host.listen_uri(), "c-browser").expect("connect");
    let _ = client.recv_text();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut received = Vec::new();
        let mut wire = Some(client);
        drain_chat_event_deltas(&mut wire, &mut received);
        let _ = tx.send(received.len());
    });
    rx.recv_timeout(std::time::Duration::from_millis(800))
        .expect("drain_chat_event_deltas must not block past a deadline on a live idle socket");
}

#[test]
fn wire_ingress_waits_for_owner_tick_and_preserves_runtime_budget() {
    let runtime = SharedRuntime::new();
    let keys = generate_keys();
    let (observer_tx, observer_rx) = bounded_channel(MAX_CHAT_INPUTS_PER_TICK);
    let host = EntityChatHost::new(
        RECONNECT_WINDOW_MS,
        SharedClock::test(),
        Box::new(runtime.clone()),
        Box::new(TestKernel::new()),
        ADMISSION_KEY_ID,
        keys.public.to_vec(),
        1_000,
    );
    host.attach_wire_input_observer(observer_tx);
    let admit = host.admit(
        "room-main".to_owned(),
        "c-bot01".to_owned(),
        credential(&keys, "Bot01", true),
    );
    assert!(admit.accepted);
    let mut client = RoomClient::connect(&host.listen_uri(), "c-bot01").expect("connect");
    let _ = client.recv_text();
    send_n_wire_chats(&mut client, MAX_CHAT_INPUTS_PER_TICK);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    for _ in 0..MAX_CHAT_INPUTS_PER_TICK {
        observer_rx
            .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
            .expect("all inputs reach the owner before the explicit tick");
    }
    // This owner-thread round trip also orders the assertion after the last
    // on_wire callback. Observing bytes alone happens before that callback ends.
    assert!(host.try_self_lookup("c-bot01".to_owned()).is_some());
    assert!(
        runtime.lock().run_tick_input_counts().is_empty(),
        "network load must not advance logical time"
    );
    assert!(host.run_tick("room-main".to_owned()).ok);
    let counts = runtime.lock().run_tick_input_counts().to_vec();
    assert_eq!(counts, vec![MAX_CHAT_INPUTS_PER_TICK]);
}

#[test]
fn second_c_browser_attach_still_receives_room_delta() {
    let keys = generate_keys();
    let host = EntityChatHost::new(
        RECONNECT_WINDOW_MS,
        SharedClock::test(),
        Box::new(SharedRuntime::new()),
        Box::new(TestKernel::new()),
        ADMISSION_KEY_ID,
        keys.public.to_vec(),
        1_000,
    );
    let admit = host.admit(
        "room-main".to_owned(),
        "c-browser".to_owned(),
        credential(&keys, "Browser01", false),
    );
    assert!(admit.accepted);
    let mut first = RoomClient::connect(&host.listen_uri(), "c-browser").expect("first");
    let _ = first.recv_text();
    let mut second = RoomClient::connect(&host.listen_uri(), "c-browser").expect("second");
    let _ = second.recv_text();
    let _ = host.admit_input_command("c-browser".to_owned(), runtime_wire_chat_input());
    let tick = host.run_tick("room-main".to_owned());
    assert!(tick.ok);
    let first_frame = first.recv_text().expect("first delta");
    let second_frame = second.recv_text().expect("second delta");
    assert!(
        first_frame.contains("\"messageType\":\"WorldChange\""),
        "first c-browser observer must keep receiving WorldChange, got {first_frame}"
    );
    assert!(
        second_frame.contains("\"messageType\":\"WorldChange\""),
        "Playwright-style second c-browser attach must also receive WorldChange, got {second_frame}"
    );
}

#[test]
fn closing_one_observer_keeps_the_other_logical_connection_alive() {
    let keys = generate_keys();
    let host = EntityChatHost::new(
        RECONNECT_WINDOW_MS,
        SharedClock::test(),
        Box::new(SharedRuntime::new()),
        Box::new(TestKernel::new()),
        ADMISSION_KEY_ID,
        keys.public.to_vec(),
        1_000,
    );
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-browser".to_owned(),
            credential(&keys, "Browser01", false),
        )
        .accepted
    );
    let mut first = RoomClient::connect(&host.listen_uri(), "c-browser").expect("first");
    let _ = first.recv_text();
    let mut second = RoomClient::connect(&host.listen_uri(), "c-browser").expect("second");
    let _ = second.recv_text();
    drop(first);
    std::thread::sleep(std::time::Duration::from_millis(80));
    let _ = host.run_tick("room-main".to_owned());
    assert!(host.try_self_lookup("c-browser".to_owned()).is_some());
}

#[test]
fn oversized_post_admission_text_closes_socket_before_runtime_input() {
    let runtime = SharedRuntime::new();
    let (host, keys) = host_ready(runtime.clone());
    assert!(
        host.admit(
            "room-main".to_owned(),
            "c-oversized".to_owned(),
            credential(&keys, "Bot01", true),
        )
        .accepted
    );
    let mut client = RoomClient::connect(&host.listen_uri(), "c-oversized").expect("connect");
    let _ = client.recv_text();
    client
        .send_text(&"a".repeat(MAX_WIRE_TEXT_BYTES + 1))
        .expect("wire send reaches server boundary");
    assert!(client.is_closed_after());
    assert!(runtime.lock().run_tick_input_counts().is_empty());
}
