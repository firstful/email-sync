// email-ingest-worker — CF Email Routing → R2 (raw) + Queue (guid)
//
// Receives raw MIME via the Email Workers `email` handler, writes the raw
// message to an R2 bucket under a new GUID, and enqueues {guid, envelope} to
// a CF Queue. The homelab email-sync service drains the queue (fetch R2,
// store, trigger rules) and reconciles R2 on boot for durability beyond the
// queue's retention window.
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

    // Durable store: write raw MIME to R2 under the guid.
    await env.RAW_BUCKET.put(guid, raw, {
      httpMetadata: { contentType: "message/rfc822" },
      customMetadata: {
        to,
        from: (message.from || "").toLowerCase(),
        subject: (message.headers.get("subject") || "").slice(0, 256),
      },
    });

    // Fast-path notification: enqueue the guid (+ minimal envelope for logging).
    await env.EMAIL_QUEUE.send({
      guid,
      to,
      from: (message.from || "").toLowerCase(),
      subject: message.headers.get("subject") || "",
      date: message.headers.get("date") || "",
    });
  },
};
