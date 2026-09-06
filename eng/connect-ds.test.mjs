import test from "node:test";
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
