// email-ingest-worker — CF Email Routing → R2 (raw blob) + Queue (guid)
//
// Receives raw MIME via the Email Workers `email` handler, writes the raw
// message to Cloudflare R2 under a new GUID, and enqueues {guid, envelope} to
// a CF Queue. The homelab email-sync service drains the queue (fetch blob,
// store locally, trigger rules) and reconciles R2 on boot, so mail survives
// any homelab downtime.
//
// No direct POST to the homelab: delivery no longer depends on homelab uptime.

const MAX_BYTES = 4 * 1024 * 1024; // must match gateway IngestMaxBytes

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

    // Durable store: the complete raw MIME message in R2.
    await env.RAW_BUCKET.put(guid + ".eml", raw, {
      httpMetadata: { contentType: "message/rfc822" },
    });

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
