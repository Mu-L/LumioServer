/** Open a DS socket with a room-bound Platform Launch credential.
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
