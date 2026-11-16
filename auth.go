package main

import (
	"bytes"
	"net/mail"
	"strings"
)

// This file implements DMARC-gated sender verification for the rules engine.
//
// Threat model: the envelope From: header is trivially spoofable. A rule that
// fires an agent session (which can execute ops instructions) must therefore
// require cryptographic proof that the claimed sender really sent the message.
// The proof we use is the DMARC result stamped by OUR receiving MX at the edge:
//
//	Authentication-Results: mx.cloudflare.net;
//	    dkim=pass header.d=gmail.com header.s=...;
//	    dmarc=pass header.from=gmail.com policy.dmarc=none
//
// Security properties:
//   - We pin the authserv-id to the trusted resolver (default mx.cloudflare.net)
//     via AUTH_TRUSTED_RESOLVER. Results from ANY other resolver (mx.google.com,
//     mx.microsoft.com, or an attacker-supplied header) are ignored.
//   - We take the FIRST (topmost) Authentication-Results header with that id.
//     Our MX prepends its header when it receives the message, so any header an
//     attacker injected upstream sits BELOW ours in the header block and cannot
//     spoof a pass. "First trusted header wins" is the standard SPF/DMARC check.
//   - We require dmarc=pass with header.from aligned to the From address domain.
//     DMARC pass already means SPF or DKIM aligned with the From domain.
//   - Everything fails closed: no header, unparseable MIME, empty raw, or no
//     match => false. A trusted-sender rule will simply not fire.
//
// This is a HARD gate in Go (not a suggestion in a prompt): the body is never
// consulted and prompt-injection in the mail cannot bypass it.

// authVerifiedSender reports whether fromAddr is backed by an aligned DMARC
// pass from the trusted receiving MX. raw is the full RFC822 stored for the
// message; resolver is the authserv-id we trust (mx.cloudflare.net).
func authVerifiedSender(raw []byte, fromAddr, resolver string) bool {
	if len(raw) == 0 || resolver == "" || fromAddr == "" {
		return false
	}
	domain := addrDomain(fromAddr)
	if domain == "" {
		return false
	}
	ar, ok := trustedAuthResults(raw, resolver)
	if !ok {
		return false
	}
	return dmarcPassAligned(ar, domain)
}

// trustedAuthResults returns the value of the topmost Authentication-Results
// header whose authserv-id equals resolver (case-insensitive).
func trustedAuthResults(raw []byte, resolver string) (string, bool) {
	msg, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return "", false
	}
	// mail.Header is a map with canonicalized keys; collect all values for the
	// Authentication-Results key case-insensitively. Order matters: the first
	// trusted-header value in the header block wins (see file header comment).
	for key, values := range msg.Header {
		if !strings.EqualFold(key, "Authentication-Results") {
			continue
		}
		for _, v := range values {
			toks := strings.Fields(v)
			if len(toks) == 0 {
				continue
			}
			// authserv-id is the first token; it is usually followed by ';'.
			id := strings.TrimSuffix(strings.TrimSpace(toks[0]), ";")
			if strings.EqualFold(id, resolver) {
				return v, true
			}
		}
	}
	return "", false
}

// dmarcPassAligned parses one Authentication-Results value and returns true
// iff it contains a "dmarc=pass" method with a header.from value aligned with
// domain (exact or relaxed/subdomain match).
func dmarcPassAligned(arValue, domain string) bool {
	for _, part := range strings.Split(arValue, ";") {
		toks := strings.Fields(part)
		if len(toks) == 0 {
			continue
		}
		kv := strings.SplitN(toks[0], "=", 2)
		if len(kv) != 2 || !strings.EqualFold(kv[0], "dmarc") || !strings.EqualFold(kv[1], "pass") {
			continue
		}
		// DMARC result found and == pass; now require aligned header.from.
		for _, tok := range toks[1:] {
			hf := strings.SplitN(tok, "=", 2)
			if len(hf) != 2 || !strings.EqualFold(hf[0], "header.from") {
				continue
			}
			hd := strings.ToLower(strings.TrimSpace(hf[1]))
			// Exact match only. header.from in a DMARC pass result is the
			// From-domain CF evaluated; require it equal the sender's domain.
			// Relaxed subdomain matching would let ally@gmail.com pass a
			// reedsut@gmail.com rule if gmail.com ever allowed subdomains.
			if hd == domain {
				return true
			}
		}
	}
	return false
}

// addrDomain extracts the lowercased domain from an email address, tolerating
// a trailing '>' from a malformed From header.
func addrDomain(addr string) string {
	i := strings.LastIndex(addr, "@")
	if i < 0 || i == len(addr)-1 {
		return ""
	}
	return strings.ToLower(strings.Trim(addr[i+1:], "> "))
}
