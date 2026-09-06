//! Real sockets against the production constructor; Runtime is an explicit test double.
mod common;
use common::{SharedRuntime, TestKernel};
use lumio_host_runtime::SharedClock;
use lumio_server_process::entity_chat::{
    generate_keys, issue_bound_test_credential, AllocationContext, BoundAdmissionVerifier,
    EntityChatHost, RoomClient,
};

fn context() -> AllocationContext {
    AllocationContext {
        server_audience: "ds-a".into(),
        game_id: "game-a".into(),
        game_release_id: "release-a".into(),
        contract_id: "lumio.gameplay-envelope.v1".into(),
        room_id: "room-a".into(),
        allocation_id: "allocation-a".into(),
    }
}
#[test]
fn arbitrary_socket_id_is_not_an_authorization_capability() {
    let keys = generate_keys();
    let allocation = context();
    let ticket = issue_bound_test_credential(&keys.seed, &allocation, 2000);
    let verifier = BoundAdmissionVerifier::new(
        allocation,
        1,
        keys.public.to_vec(),
        SharedClock::test(),
        1000,
    )
    .unwrap();
    let host = EntityChatHost::new_authenticated(
        300_000,
        Box::new(SharedRuntime::new()),
        Box::new(TestKernel::new()),
        verifier,
    )
    .unwrap();
    assert!(
        host.admit(
            "room-a".to_owned(),
            "known-connection".to_owned(),
            ticket.clone()
        )
        .accepted
    );
    assert!(RoomClient::connect(&host.listen_uri(), "known-connection").is_err());
    assert!(RoomClient::connect_authenticated(&host.listen_uri(), "bad-ticket").is_err());
    assert!(host.is_healthy());
}
#[test]
fn valid_socket_uses_server_allocated_identity_and_gets_runtime_welcome() {
    let keys = generate_keys();
    let allocation = context();
    let ticket = issue_bound_test_credential(&keys.seed, &allocation, 2000);
    let verifier = BoundAdmissionVerifier::new(
        allocation,
        1,
        keys.public.to_vec(),
        SharedClock::test(),
        1000,
    )
    .unwrap();
    let host = EntityChatHost::new_authenticated(
        300_000,
        Box::new(SharedRuntime::new()),
        Box::new(TestKernel::new()),
        verifier,
    )
    .unwrap();
    let mut client = RoomClient::connect_authenticated(&host.listen_uri(), &ticket)
        .expect("authenticated socket");
    assert!(client.recv_text().expect("welcome").contains("Welcome"));
    assert!(host
        .try_self_lookup("known-connection".to_owned())
        .is_none());
}
#[test]
fn stalled_handshake_cannot_hold_host_drop_open() {
    let keys = generate_keys();
    let verifier = BoundAdmissionVerifier::new(
        context(),
        1,
        keys.public.to_vec(),
        SharedClock::test(),
        1000,
    )
    .unwrap();
    let host = EntityChatHost::new_authenticated(
        300_000,
        Box::new(SharedRuntime::new()),
        Box::new(TestKernel::new()),
        verifier,
    )
    .unwrap();
    let address = host.listen_uri().trim_start_matches("ws://").to_owned();
    let _socket = std::net::TcpStream::connect(address).unwrap();
    let start = std::time::Instant::now();
    drop(host);
    assert!(start.elapsed() < std::time::Duration::from_secs(2));
}

#[test]
fn browser_upgrade_never_echoes_the_credential_offer() {
    use tokio_tungstenite::tungstenite::{
        client::connect, client::IntoClientRequest, http::HeaderValue,
    };
    let keys = generate_keys();
    let allocation = context();
    let ticket = issue_bound_test_credential(&keys.seed, &allocation, 2000);
    let verifier = BoundAdmissionVerifier::new(
        allocation,
        1,
        keys.public.to_vec(),
        SharedClock::test(),
        1000,
    )
    .unwrap();
    let host = EntityChatHost::new_authenticated(
        300_000,
        Box::new(SharedRuntime::new()),
        Box::new(TestKernel::new()),
        verifier,
    )
    .unwrap();
    let mut request = host.listen_uri().into_client_request().unwrap();
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_str(&format!("lumio.mvp.v0, lumio-admission.{ticket}")).unwrap(),
    );
    let (_socket, response) = connect(request).expect("browser style upgrade");
    assert_eq!(
        response.headers().get("Sec-WebSocket-Protocol").unwrap(),
        "lumio.mvp.v0"
    );
    assert!(!format!("{:?}", response.headers()).contains(&ticket));
}
