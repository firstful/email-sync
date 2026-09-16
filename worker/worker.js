// email-ingest-worker — CF Email Routing → Hermes email-sync gateway /ingest
//
// Receives raw MIME via the Email Workers `email` handler, POSTs it to the
// gateway with the ingest token + CF Access service-token headers.
// Policy (Reed 09-16):
//   * EVERY message addressed to @ffilt.us is ingested to the searchable DB —
//     regardless of the local-part (catch-all ingest).
//   * Forwarding to the Namecheap mailbox is selective (see FORWARD_TO below).
//   * Session triggers are decided by the gateway's rules engine (whitelist),
//     NOT here — worker never drops by content.

const GATEWAY = "https://email.ffilt.us/ingest";
const MAX_BYTES = 4 * 1024 * 1024; // must match gateway IngestMaxBytes

// Local-parts that ALSO forward to the Namecheap mailbox (Reed's real inbox).
const FORWARD_LOCALPARTS = new Set(["reed"]);

// Destination must exist as a VERIFIED CF Email Routing destination address.
const FORWARD_DOMAIN = "ffilt.us";

export default {
  async email(message, env, ctx) {
    const to = (message.to || "").toLowerCase();
    const from = (message.from || "").toLowerCase();

    // Ingest EVERYTHING addressed to the domain (defense in depth: Email
    // Routing only routes ffilt.us addresses anyway).
    if (!to.endsWith("@" + FORWARD_DOMAIN)) {
      message.setReject("recipient domain not accepted");
      return;
    }

    const localpart = to.slice(0, -("@ffilt.us".length));

    // Size cap: don't ingest monsters.
    const raw = await new Response(message.raw).arrayBuffer();
    if (raw.byteLength > MAX_BYTES) {
      message.setReject("message too large");
      return;
    }

    // Ingest (all mail). Never fails the message on gateway errors — log and
    // continue so forwarding still happens.
    let ingestOk = false;
    try {
      console.error("DEBUG: posting", raw.byteLength, "bytes to", GATEWAY);
      const resp = await fetch(GATEWAY, {
        method: "POST",
        headers: {
          "X-Ingest-Token": env.INGEST_TOKEN,
          "CF-Access-Client-Id": env.CF_ACCESS_CLIENT_ID,
          "CF-Access-Client-Secret": env.CF_ACCESS_CLIENT_SECRET,
          "Content-Type": "message/rfc822",
          "X-Original-To": to,
          "X-Original-From": from,
        },
        body: raw,
      });
      console.error("DEBUG: gateway said", resp.status, (await resp.text()).slice(0, 200));
      ingestOk = resp.ok;
    } catch (e) {
      console.error("DEBUG: fetch exception", (e && e.stack) || e);
    }

    // Forward selectively (Reed's mailbox only).
    if (FORWARD_LOCALPARTS.has(localpart)) {
      message.forward(localpart + "@" + FORWARD_DOMAIN); // same addr — CF delivers to the NEXT MX... see note
    }
  },
};
