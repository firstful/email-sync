package main

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/mail"
	"strings"
)

func parseAttachmentsJSON(raw string) []Attachment {
	var out []Attachment
	if raw == "" {
		return out
	}
	_ = json.Unmarshal([]byte(raw), &out)
	return out
}

func parseAttachments(raw []byte) []Attachment {
	var atts []Attachment
	msg, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return atts
	}
	mediaType, params, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
	if err != nil {
		mediaType = "text/plain"
	}
	if !strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
		cd := msg.Header.Get("Content-Disposition")
		disp, dparams, _ := mime.ParseMediaType(cd)
		if strings.EqualFold(disp, "attachment") {
			filename := dparams["filename"]
			if filename == "" {
				_, cparams, _ := mime.ParseMediaType(msg.Header.Get("Content-Type"))
				filename = cparams["name"]
			}
			if filename == "" {
				filename = "unnamed"
			}
			ct := msg.Header.Get("Content-Type")
			if ct == "" {
				ct = "application/octet-stream"
			}
			if i := strings.Index(ct, ";"); i >= 0 {
				ct = strings.TrimSpace(ct[:i])
			}
			atts = append(atts, Attachment{Filename: filename, ContentType: ct, Size: len(raw)})
		}
		return atts
	}
	boundary := params["boundary"]
	if boundary == "" {
		return atts
	}
	mr := multipart.NewReader(msg.Body, boundary)
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		disp := part.Header.Get("Content-Disposition")
		disposition, dparams, _ := mime.ParseMediaType(disp)
		filename := dparams["filename"]
		if filename == "" {
			ct := part.Header.Get("Content-Type")
			_, cparams, _ := mime.ParseMediaType(ct)
			filename = cparams["name"]
		}
		isAttachment := strings.EqualFold(disposition, "attachment") || filename != ""
		data, _ := io.ReadAll(part)
		if !isAttachment {
			continue
		}
		if filename == "" {
			filename = "unnamed"
		}
		ct := part.Header.Get("Content-Type")
		if ct == "" {
			ct = "application/octet-stream"
		}
		if i := strings.Index(ct, ";"); i >= 0 {
			ct = strings.TrimSpace(ct[:i])
		}
		atts = append(atts, Attachment{Filename: filename, ContentType: ct, Size: len(data)})
	}
	return atts
}