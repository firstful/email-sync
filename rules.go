package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"text/template"
	"time"

	"gopkg.in/yaml.v3"
)

// Rule is one declarative inbound-mail rule (rules.yaml mounted read-only).
type Rule struct {
	Name  string `yaml:"name"`
	Match struct {
		From    string `yaml:"from"`    // regex, matched against envelope from_addr
		To      string `yaml:"to"`      // regex, matched against to_addr
		Subject string `yaml:"subject"` // regex, matched against subject
	} `yaml:"match"`
	Action struct {
		Type    string `yaml:"type"` // "session" (only type today)
		Profile string `yaml:"profile"`
		Prompt  string `yaml:"prompt"` // Go text/template
	} `yaml:"action"`
}

// RulesConfig is the parsed rules file.
type RulesConfig struct {
	Rules []Rule `yaml:"rules"`
}

// RuleEngine evaluates ingested messages against RulesConfig and fires
// Hermes-session triggers. Payloads carry METADATA ONLY (email_id + envelope);
// the session pulls content via the mail_* MCP tool — untrusted email content
// never rides in the trigger payload.
type RuleEngine struct {
	mu      sync.RWMutex
	rules   []compiledRule
	webhook string // Hermes API server trigger URL
	token   string // webhook auth token
	client  *http.Client
}

type compiledRule struct {
	rule      Rule
	fromRe    *regexp.Regexp
	toRe      *regexp.Regexp
	subjRe    *regexp.Regexp
	promptTpl *template.Template
}

func NewRuleEngine(webhook, token string) *RuleEngine {
	return &RuleEngine{
		webhook: webhook,
		token:   token,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

// LoadRules parses rules YAML; missing file = empty rules (store-only mode).
func (e *RuleEngine) LoadRules(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("rules: %s not found — store-only mode", path)
			e.mu.Lock()
			e.rules = nil
			e.mu.Unlock()
			return nil
		}
		return err
	}
	var cfg RulesConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("rules parse: %w", err)
	}
	compiled := make([]compiledRule, 0, len(cfg.Rules))
	for _, r := range cfg.Rules {
		cr := compiledRule{rule: r}
		var err error
		if r.Match.From != "" {
			if cr.fromRe, err = regexp.Compile("(?i)" + r.Match.From); err != nil {
				return fmt.Errorf("rule %q from regex: %w", r.Name, err)
			}
		}
		if r.Match.To != "" {
			if cr.toRe, err = regexp.Compile("(?i)" + r.Match.To); err != nil {
				return fmt.Errorf("rule %q to regex: %w", r.Name, err)
			}
		}
		if r.Match.Subject != "" {
			if cr.subjRe, err = regexp.Compile("(?i)" + r.Match.Subject); err != nil {
				return fmt.Errorf("rule %q subject regex: %w", r.Name, err)
			}
		}
		prompt := r.Action.Prompt
		if prompt == "" {
			prompt = "Inbound email {{.EmailID}} matched rule {{.Name}} (from {{.From}}). Read it via the mail MCP tool and act as appropriate."
		}
		tpl, err := template.New("prompt").Parse(prompt)
		if err != nil {
			return fmt.Errorf("rule %q prompt template: %w", r.Name, err)
		}
		cr.promptTpl = tpl
		compiled = append(compiled, cr)
	}
	e.mu.Lock()
	e.rules = compiled
	e.mu.Unlock()
	log.Printf("rules: loaded %d rule(s)", len(compiled))
	return nil
}

// Dispatch evaluates one ingested message against all rules; matches fire async.
func (e *RuleEngine) Dispatch(m *Message) {
	e.mu.RLock()
	rules := e.rules
	e.mu.RUnlock()
	for _, cr := range rules {
		if !cr.matches(m) {
			continue
		}
		go e.fire(cr, m)
	}
}

func (c *compiledRule) matches(m *Message) bool {
	if c.fromRe != nil && !c.fromRe.MatchString(m.FromAddr) {
		return false
	}
	if c.toRe != nil && !c.toRe.MatchString(m.To) {
		return false
	}
	if c.subjRe != nil && !c.subjRe.MatchString(m.Subject) {
		return false
	}
	return c.fromRe != nil || c.toRe != nil || c.subjRe != nil
}

// triggerPayload is deliberately metadata-only — no raw email content.
// Shape matches the Hermes API server runs API (POST /p/<profile>/v1/runs).
type triggerPayload struct {
	Input string `json:"input"`
}

func (e *RuleEngine) fire(cr compiledRule, m *Message) {
	data := struct {
		Name    string
		EmailID uint32
		From    string
		Subject string
	}{cr.rule.Name, m.UID, sanitizeHeader(m.FromAddr), sanitizeHeader(m.Subject)}
	var buf bytes.Buffer
	if err := cr.promptTpl.Execute(&buf, data); err != nil {
		log.Printf("rule %q: prompt render: %v", cr.rule.Name, err)
		return
	}
	payload := triggerPayload{
		Input: fmt.Sprintf("[inbound email #%d from %s subject %q] %s", m.UID, data.From, data.Subject, buf.String()),
	}
	body, _ := json.Marshal(payload)
	url := strings.TrimRight(e.webhook, "/") + "/v1/runs"
	if p := cr.rule.Action.Profile; p != "" && p != "default" {
		url = strings.TrimRight(e.webhook, "/") + "/p/" + p + "/v1/runs"
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if e.token != "" {
		req.Header.Set("Authorization", "Bearer "+e.token)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := e.client.Do(req.WithContext(ctx))
	if err != nil {
		log.Printf("rule %q: webhook: %v", cr.rule.Name, err)
		return
	}
	resp.Body.Close()
	log.Printf("rule %q: session triggered for email #%d (webhook %d)", cr.rule.Name, m.UID, resp.StatusCode)
}
