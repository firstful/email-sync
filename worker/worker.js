// email-ingest-worker — CF Email Routing → D1 (raw blob) + Queue (guid)
//
// Receives raw MIME via the Email Workers `email` handler, writes the raw
// message to Cloudflare D1 under a new GUID, and enqueues {guid, envelope} to
// a CF Queue. The homelab email-sync service drains the queue (fetch blob,
// store locally, trigger rules) and reconciles D1 on boot, so mail survives
// any homelab downtime.
//
// No direct POST to the homelab: delivery no longer depends on homelab uptime.

const MAX_BYTES = 4 * 1024 * 1024; // must match gateway IngestMaxBytes
const CHUNK_SIZE = 512 * 1024;     // base64 chars per chunk row

// b64encode encodes bytes to base64 in chunks that avoid blowing the JS
// argument limit for large messages.
function b64encode(bytes) {
  let binary = "";
  const step = 0x8000;
  for (let i = 0; i < bytes.length; i += step) {
    binary += String.fromCharCode.apply(null, bytes.subarray(i, i + step));
  }
  return btoa(binary);
}

export default {
  async email(message, env, ctx) {
    const to = (message.to || "").toLowerCase();
    const domain = "ffilt.us";

    if (!to.endsWith("@" + domain)) {
      message.setReject("recipient domain not accepted");
      return;
    }

    const raw = await new Response(message.raw).arrayBuffer();
    if (raw.byteLength > MAX_BYTES) {
      message.setReject("message too large");
      return;
    }

    const guid = crypto.randomUUID();
    const from = (message.from || "").toLowerCase();
    const subject = message.headers.get("subject") || "";
    const b64 = b64encode(new Uint8Array(raw));

    // Durable store: raw MIME chunks + metadata row in D1.
    const chunkCount = Math.ceil(b64.length / CHUNK_SIZE);
    const stmts = [];
    for (let i = 0; i < chunkCount; i++) {
      stmts.push(
        env.DB.prepare(
          "INSERT OR REPLACE INTO raw_chunks (guid, seq, data) VALUES (?, ?, ?)"
        ).bind(guid, i, b64.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE))
      );
    }
    stmts.push(
      env.DB.prepare(
        "INSERT OR REPLACE INTO raw_meta (guid, size, chunks, received_at, from_addr, to_addr, subject) VALUES (?, ?, ?, ?, ?, ?, ?)"
      ).bind(guid, raw.byteLength, chunkCount, Date.now(), from, to, subject.slice(0, 256))
    );
    await env.DB.batch(stmts);

    // Fast-path notification: enqueue the guid (+ minimal envelope for logging).
    await env.EMAIL_QUEUE.send({
      guid,
      to,
      from,
      subject: subject.slice(0, 256),
      date: message.headers.get("date") || "",
    });
  },
};
