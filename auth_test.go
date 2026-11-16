package main

import "testing"

// Realistic CF-stamped header taken from an organically received message in
// this mailbox (uid 1007); authserv-id is our trusted edge MX.
const cfAuthPass = "Authentication-Results: mx.cloudflare.net;\r\n" +
	"\tdkim=pass header.d=gmail.com header.s=20260707 header.b=QhRQQX2H;\r\n" +
	"\tdmarc=pass header.from=gmail.com policy.dmarc=none;\r\n"

func wrapHeaders(authHeaders, from string) []byte {
	s := "From: Reed <" + from + ">\r\n" +
		"To: ingest@ffilt.us\r\n" +
		"Subject: test\r\n" +
		authHeaders +
		"\r\nbody\r\n"
	return []byte(s)
}

func TestAuthVerifiedSender(t *testing.T) {
	const resolver = "mx.cloudflare.net"
	cases := []struct {
		name     string
		raw      []byte
		from     string
		resolver string
		want     bool
	}{
		{
			name:     "genuine gmail dmarc pass",
			raw:      wrapHeaders(cfAuthPass, "reedsutliff@gmail.com"),
			from:     "reedsutliff@gmail.com",
			resolver: resolver,
			want:     true,
		},
		{
			name: "genuine foxen dmarc pass",
			raw: wrapHeaders("Authentication-Results: mx.cloudflare.net;\r\n"+
				"\tdkim=pass header.d=foxen.com;\r\n"+
				"\tdmarc=pass header.from=foxen.com policy.dmarc=quarantine;\r\n", "rsutliff@foxen.com"),
			from:     "rsutliff@foxen.com",
			resolver: resolver,
			want:     true,
		},
		{
			name: "spoofed From with attacker-supplied auth header (untrusted resolver)",
			raw: wrapHeaders("Authentication-Results: evil.example;\r\n"+
				"\tdmarc=pass header.from=gmail.com;\r\n", "attacker@evil.com"),
			from:     "reedsutliff@gmail.com", // claims Reed, no CF header
			resolver: resolver,
			want:     false,
		},
		{
			name: "CF header present but dmarc from domain mismatches From",
			raw: wrapHeaders("Authentication-Results: mx.cloudflare.net;\r\n"+
				"\tdmarc=pass header.from=evil.com;\r\n", "reedsutliff@gmail.com"),
			from:     "reedsutliff@gmail.com",
			resolver: resolver,
			want:     false,
		},
		{
			name: "CF header with dmarc=fail",
			raw: wrapHeaders("Authentication-Results: mx.cloudflare.net;\r\n"+
				"\tdmarc=fail header.from=gmail.com;\r\n", "reedsutliff@gmail.com"),
			from:     "reedsutliff@gmail.com",
			resolver: resolver,
			want:     false,
		},
		{
			name: "dkim pass alone is not enough (no dmarc)",
			raw: wrapHeaders("Authentication-Results: mx.cloudflare.net;\r\n"+
				"\tdkim=pass header.d=gmail.com;\r\n", "reedsutliff@gmail.com"),
			from:     "reedsutliff@gmail.com",
			resolver: resolver,
			want:     false,
		},
		{
			name:     "empty raw",
			raw:      nil,
			from:     "reedsutliff@gmail.com",
			resolver: resolver,
			want:     false,
		},
		{
			name:     "empty resolver fails closed",
			raw:      wrapHeaders(cfAuthPass, "reedsutliff@gmail.com"),
			from:     "reedsutliff@gmail.com",
			resolver: "",
			want:     false,
		},
		{
			name: "topmost trusted header wins: attacker prepends own pass below CF fail",
			raw: wrapHeaders("Authentication-Results: mx.cloudflare.net;\r\n"+
				"\tdmarc=fail header.from=gmail.com;\r\n"+
				"Authentication-Results: mx.cloudflare.net;\r\n"+
				"\tdmarc=pass header.from=gmail.com;\r\n", "reedsutliff@gmail.com"),
			from:     "reedsutliff@gmail.com",
			resolver: resolver,
			want:     false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := authVerifiedSender(tc.raw, tc.from, tc.resolver)
			if got != tc.want {
				t.Fatalf("authVerifiedSender() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAddrDomain(t *testing.T) {
	for in, want := range map[string]string{
		"reedsutliff@gmail.com":             "gmail.com",
		"Reed Sutliff <rsutliff@foxen.com>": "foxen.com",
		"nope":                              "",
		"x@":                                "",
	} {
		if got := addrDomain(in); got != want {
			t.Errorf("addrDomain(%q) = %q, want %q", in, got, want)
		}
	}
}
