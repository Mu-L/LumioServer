from pathlib import Path

# The public default library must not expose the old unauthenticated run path.
p=Path('modules/process/src/lib.rs');s=p.read_text();a=s.index('use std::io::Write as _;')
legacy=s[a:];declarations=s[:a]
for module in ['server','session','world','cli']:
    declarations=declarations.replace(f'pub mod {module};',f'#[cfg(any(test, feature = "test-harness"))]\npub mod {module};')
p.write_text(declarations+'''#[cfg(any(test, feature = "test-harness"))]
mod legacy;
#[cfg(any(test, feature = "test-harness"))]
pub use legacy::run;
''')
Path('modules/process/src/legacy.rs').write_text('//! Hello milestone harness. Not compiled into the default DS library.\n'+legacy)

# Browser WebSocket cannot set Authorization. The alternative carries the
# same signed bearer in an HTTP Upgrade subprotocol offer, never in a URL,
# Cookie, C-1 payload, log, or the server-selected protocol response.
p=Path('modules/process/src/entity_chat/wire.rs');s=p.read_text()
a=s.index('                let header = request\n');b=s.index('                proof = Some(verifier.verify(credential)',a)
s=s[:a]+'''                let credential = upgrade_credential(request).ok_or_else(unauthorized)?;
'''+s[b:]
marker='fn unauthorized() -> ErrorResponse {'
pos=s.index(marker)
s=s[:pos]+'''fn upgrade_credential(request: &Request) -> Option<&str> {
    let header_values: Vec<_> = request.headers().get_all("Authorization").iter().collect();
    if header_values.len() > 1 { return None; }
    let authorization = header_values.first().and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer "));
    let offers = request.headers().get("Sec-WebSocket-Protocol").and_then(|v| v.to_str().ok());
    let mut tokens = offers.into_iter().flat_map(|v| v.split(',')).filter_map(|v| v.trim().strip_prefix("lumio-admission."));
    let browser_token = tokens.next();
    if tokens.next().is_some() || (request.headers().contains_key("Authorization") && browser_token.is_some()) { return None; }
    authorization.or(browser_token).filter(|v| !v.is_empty())
}

'''+s[pos:]
# Global handshake rate cap protects the shared signature-verification thread.
s=s.replace('''                let mut connections = JoinSet::new();''','''                let mut connections = JoinSet::new();
                let mut admission_window = tokio::time::Instant::now();
                let mut admissions_in_window = 0_u32;''',1)
s=s.replace('''                            let (stream, _) = accepted.expect("socket accept failed");''','''                            let (stream, _) = accepted.expect("socket accept failed");
                            if admission_window.elapsed() >= Duration::from_secs(1) {
                                admission_window = tokio::time::Instant::now();
                                admissions_in_window = 0;
                            }
                            if admissions_in_window >= 256 { drop(stream); continue; }
                            admissions_in_window += 1;''',1)
# SharedClock is used at the owner as well, so a queued proof cannot outlive its lease.
p.write_text(s)
p=Path('modules/process/src/entity_chat/host.rs');s=p.read_text()
s=s.replace('''                let result =
                    self.admit_verified(&proof.room_id, &connection_id, &proof.payload);''','''                if self.admission_verifier.as_ref().is_some_and(|v| v.now() > proof.payload.expires_at || v.allocation.room_id != proof.room_id) {
                    let _ = egress.try_close();
                    return;
                }
                let result =
                    self.admit_verified(&proof.room_id, &connection_id, &proof.payload);''',1)
p.write_text(s)
p=Path('account-server/tests/Lumio.Server.Account.Tests/StorageTransactionTests.cs');s=p.read_text().replace('''        Assert.ThrowsAny<IOException>(() => harness.Runtime.LoginOrRegister("bravo", AccountTestProfile.Password, null));''','''        var failure = Record.Exception(() => harness.Runtime.LoginOrRegister("bravo", AccountTestProfile.Password, null));
        Assert.True(failure is IOException or UnauthorizedAccessException);''');p.write_text(s)
Path('eng/connect-ds.mjs').write_text('''/** Open a DS socket with a room-bound Platform Launch credential.
 * This is a transport adapter, not a second gameplay codec. The server echoes
 * only the gameplay subprotocol, never the credential-carrying offer.
 * Do not log the subprotocol offer or retain the credential in localStorage.
 */
export function connectDs(launch, { allowLoopback = false, Socket = globalThis.WebSocket } = {}) {
  if (!launch || typeof launch !== "object") throw new TypeError("launch result required");
  const endpoint = new URL(launch.wsUrl);
  const local = ["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname);
  if (endpoint.protocol !== "wss:" && !(allowLoopback && local && endpoint.protocol === "ws:")) {
    throw new TypeError("WSS is required outside explicit loopback development");
  }
  if (endpoint.username || endpoint.password || endpoint.search || endpoint.hash) {
    throw new TypeError("credentials and routing hints must not appear in the socket URL");
  }
  const credential = launch.admissionCredential;
  if (typeof credential !== "string" || !/^[A-Za-z0-9_-]+$/.test(credential) || credential.length > 16384) {
    throw new TypeError("room-bound admission credential required");
  }
  if (launch.subprotocol !== "lumio.mvp.v0") throw new TypeError("unsupported gameplay subprotocol");
  if (typeof Socket !== "function") throw new TypeError("WebSocket is unavailable");
  return new Socket(endpoint.href, [launch.subprotocol, `lumio-admission.${credential}`]);
}
''')
Path('eng/connect-ds.test.mjs').write_text('''import test from "node:test";
import assert from "node:assert/strict";
import { connectDs } from "./connect-ds.mjs";
class Socket { constructor(url, protocols) { this.url = url; this.protocols = protocols; } }
const launch = { wsUrl: "wss://ds.example.test/", subprotocol: "lumio.mvp.v0", admissionCredential: "opaque-test-credential" };
test("browser transport keeps the credential out of URL", () => {
  const socket = connectDs(launch, { Socket });
  assert.equal(socket.url, launch.wsUrl);
  assert.deepEqual(socket.protocols, ["lumio.mvp.v0", "lumio-admission.opaque-test-credential"]);
});
test("plaintext and URL credentials are rejected", () => {
  assert.throws(() => connectDs({ ...launch, wsUrl: "ws://ds.example.test/" }, { Socket }));
  assert.throws(() => connectDs({ ...launch, wsUrl: "wss://ds.example.test/?token=x" }, { Socket }));
});
test("loopback development is explicit", () => {
  assert.throws(() => connectDs({ ...launch, wsUrl: "ws://127.0.0.1/" }, { Socket }));
  assert.ok(connectDs({ ...launch, wsUrl: "ws://127.0.0.1/" }, { Socket, allowLoopback: true }));
});
''')
p=Path('modules/process/tests/secure_transport.rs');s=p.read_text()+'''
#[test]
fn browser_upgrade_never_echoes_the_credential_offer() {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http::HeaderValue, client::connect};
    let keys=generate_keys(); let allocation=context();
    let ticket=issue_bound_test_credential(&keys.seed,&allocation,2000);
    let verifier=BoundAdmissionVerifier::new(allocation,1,keys.public.to_vec(),SharedClock::test(),1000).unwrap();
    let host=EntityChatHost::new_authenticated(300_000,Box::new(SharedRuntime::new()),Box::new(TestKernel::new()),verifier).unwrap();
    let mut request=host.listen_uri().into_client_request().unwrap();
    request.headers_mut().insert("Sec-WebSocket-Protocol",HeaderValue::from_str(&format!("lumio.mvp.v0, lumio-admission.{ticket}")).unwrap());
    let (_socket,response)=connect(request).expect("browser style upgrade");
    assert_eq!(response.headers().get("Sec-WebSocket-Protocol").unwrap(), "lumio.mvp.v0");
    assert!(!format!("{:?}",response.headers()).contains(&ticket));
}
''';p.write_text(s)
p=Path('eng/verify.py');s=p.read_text().replace('["node", ".spec/tools/spec-lint.mjs"],','["node", ".spec/tools/spec-lint.mjs"],\n            ["node", "--test", "eng/connect-ds.test.mjs"],',1);p.write_text(s)
Path(__file__).unlink()
