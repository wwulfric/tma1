package transcript

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCodexSessionGroup(t *testing.T) {
	tests := []struct {
		name     string
		baseName string
		want     string
	}{
		{
			name:     "standard rollout filename",
			baseName: "rollout-2026-03-27T18-10-59-019d2ec6-958f-7cde-b25c-acde48001122",
			want:     "rollout-2026-03-27T18-10-59",
		},
		{
			name:     "unexpected filename falls back to full name",
			baseName: "session-without-timestamp",
			want:     "session-without-timestamp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := codexSessionGroup(tt.baseName); got != tt.want {
				t.Fatalf("codexSessionGroup(%q) = %q, want %q", tt.baseName, got, tt.want)
			}
		})
	}
}

func TestCodexSubagentID(t *testing.T) {
	if got := codexSubagentID("codex:rollout-2026-03-27T18-10-59-a", "review"); got != "codex:rollout-2026-03-27T18-10-59-a" {
		t.Fatalf("codexSubagentID should prefer per-file id, got %q", got)
	}
	if got := codexSubagentID("", "review"); got != "review" {
		t.Fatalf("codexSubagentID should fall back to agent type, got %q", got)
	}
}

func TestParseCodexSessionMetaNestedSubagent(t *testing.T) {
	meta := parseCodexSessionMeta([]byte(`{"id":"conv-1","source":{"subagent":{"other":"guardian"}},"cwd":"/tmp/project"}`))
	if meta.id != "conv-1" || meta.cwd != "/tmp/project" || meta.subagent != "codex-auto-review" {
		t.Fatalf("unexpected meta: %#v", meta)
	}
}

func TestProcessCodexLineCarriesConversationIDIntoSubagentLifecycle(t *testing.T) {
	sqlCh := make(chan string, 4)
	ts := httptest.NewServer(httpTestHandler(sqlCh))
	defer ts.Close()

	oldClient := httpClient
	httpClient = ts.Client()
	defer func() { httpClient = oldClient }()

	w := &Watcher{
		sqlURL: ts.URL,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	seen := make(map[string]struct{})
	fctx := &codexFileContext{fileID: "codex:rollout-2026-03-27T18-10-59-sub"}

	w.processCodexLine("rollout-2026-03-27T18-10-59",
		`{"timestamp":"2026-03-27T18:10:59Z","type":"session_meta","payload":{"id":"conv-123","source":{"subagent":"review"},"cwd":"/tmp/project"}}`,
		seen, fctx)
	w.processCodexLine("rollout-2026-03-27T18-10-59",
		`{"timestamp":"2026-03-27T18:11:00Z","type":"event_msg","payload":{"type":"task_complete"}}`,
		seen, fctx)

	sqls := []string{waitForSQL(t, sqlCh), waitForSQL(t, sqlCh), waitForSQL(t, sqlCh)}
	var sawStart, sawStop bool
	for _, sql := range sqls {
		if !strings.Contains(sql, "conv-123") {
			t.Fatalf("expected insert to include conversation_id, got %s", sql)
		}
		if strings.Contains(sql, "SubagentStart") {
			sawStart = true
		}
		if strings.Contains(sql, "SubagentStop") {
			sawStop = true
		}
	}
	if !sawStart || !sawStop {
		t.Fatalf("expected both SubagentStart and SubagentStop inserts, got %q", sqls)
	}
}

func TestProcessCodexResponseItemEmitsToolMessages(t *testing.T) {
	sqlCh := make(chan string, 4)
	ts := httptest.NewServer(httpTestHandler(sqlCh))
	defer ts.Close()

	oldClient := httpClient
	httpClient = ts.Client()
	defer func() { httpClient = oldClient }()

	w := &Watcher{
		sqlURL: ts.URL,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	seen := make(map[string]struct{})
	fctx := &codexFileContext{model: "gpt-5.5", conversationID: "conv-tool"}

	w.processCodexLine("rollout-2026-03-27T18-10-59",
		`{"timestamp":"2026-03-27T18:11:01Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","call_id":"call-1","arguments":"{\"cmd\":\"go test ./...\"}"}}`,
		seen, fctx)
	w.processCodexLine("rollout-2026-03-27T18-10-59",
		`{"timestamp":"2026-03-27T18:11:02Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call-1","output":"ok"}}`,
		seen, fctx)

	sqls := []string{
		waitForSQL(t, sqlCh),
		waitForSQL(t, sqlCh),
		waitForSQL(t, sqlCh),
		waitForSQL(t, sqlCh),
	}
	assertAnySQLContains(t, sqls, "tma1_hook_events", "PreToolUse", "exec_command", "call-1")
	assertAnySQLContains(t, sqls, "tma1_hook_events", "PostToolUse", "call-1", "ok")
	assertAnySQLContains(t, sqls, "tma1_messages", "tool_use", "exec_command", "call-1", "gpt-5.5")
	assertAnySQLContains(t, sqls, "tma1_messages", "tool_result", "call-1", "ok", "gpt-5.5")
}

func TestProcessCodexWebSearchEndEmitsToolMessages(t *testing.T) {
	sqlCh := make(chan string, 4)
	ts := httptest.NewServer(httpTestHandler(sqlCh))
	defer ts.Close()

	oldClient := httpClient
	httpClient = ts.Client()
	defer func() { httpClient = oldClient }()

	w := &Watcher{
		sqlURL: ts.URL,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	seen := make(map[string]struct{})
	fctx := &codexFileContext{model: "gpt-5.5", conversationID: "conv-search"}

	w.processCodexLine("rollout-2026-03-27T18-10-59",
		`{"timestamp":"2026-03-27T18:11:03Z","type":"event_msg","payload":{"type":"web_search_end","call_id":"ws-1","query":"xdisp cursor","action":{"type":"search","query":"xdisp cursor"}}}`,
		seen, fctx)

	sqls := []string{
		waitForSQL(t, sqlCh),
		waitForSQL(t, sqlCh),
		waitForSQL(t, sqlCh),
		waitForSQL(t, sqlCh),
	}
	assertAnySQLContains(t, sqls, "tma1_hook_events", "PreToolUse", "web_search", "ws-1", "xdisp cursor")
	assertAnySQLContains(t, sqls, "tma1_hook_events", "PostToolUse", "web_search", "ws-1", "xdisp cursor")
	assertAnySQLContains(t, sqls, "tma1_messages", "tool_use", "web_search", "ws-1", "gpt-5.5")
	assertAnySQLContains(t, sqls, "tma1_messages", "tool_result", "web_search", "ws-1", "gpt-5.5")
}

func TestProcessCodexResponseItemEmitsReasoningSummary(t *testing.T) {
	sqlCh := make(chan string, 1)
	ts := httptest.NewServer(httpTestHandler(sqlCh))
	defer ts.Close()

	oldClient := httpClient
	httpClient = ts.Client()
	defer func() { httpClient = oldClient }()

	w := &Watcher{
		sqlURL: ts.URL,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	seen := make(map[string]struct{})
	fctx := &codexFileContext{model: "gpt-5.5"}

	w.processCodexLine("rollout-2026-03-27T18-10-59",
		`{"timestamp":"2026-03-27T18:11:03Z","type":"response_item","payload":{"type":"reasoning","summary":[{"type":"summary_text","text":"Need inspect parser."}]}}`,
		seen, fctx)

	sql := waitForSQL(t, sqlCh)
	for _, want := range []string{"tma1_messages", "thinking", "Need inspect parser.", "gpt-5.5"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected SQL to contain %q, got %s", want, sql)
		}
	}
}

func TestProcessCodexLinePreseededReplayKeepsConversationState(t *testing.T) {
	sqlCh := make(chan string, 1)
	ts := httptest.NewServer(httpTestHandler(sqlCh))
	defer ts.Close()

	oldClient := httpClient
	httpClient = ts.Client()
	defer func() { httpClient = oldClient }()

	w := &Watcher{
		sqlURL: ts.URL,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	seen := make(map[string]struct{})
	hookSeen := map[string]struct{}{
		codexHookKey("SessionStart", "", "", "", "", "", "", "conv-replay", "/tmp/project"): {},
	}
	fctx := newCodexFileContext("", hookSeen)

	w.processCodexLine("rollout-2026-03-27T18-10-59",
		`{"timestamp":"2026-03-27T18:10:59Z","type":"session_meta","payload":{"id":"conv-replay","source":"vscode","cwd":"/tmp/project"}}`,
		seen, fctx)
	w.processCodexLine("rollout-2026-03-27T18-10-59",
		`{"timestamp":"2026-03-27T18:10:59Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","call_id":"call-new","arguments":"{}"}}`,
		seen, fctx)

	sqls := []string{waitForSQL(t, sqlCh), waitForSQL(t, sqlCh)}
	assertAnySQLContains(t, sqls, "tma1_hook_events", "conv-replay", "call-new")
	for _, sql := range sqls {
		if strings.Contains(sql, "SessionStart") {
			t.Fatalf("old SessionStart should have been skipped, got %s", sql)
		}
	}
}

func TestCodexHookSeenDeduplicatesHookEvents(t *testing.T) {
	sqlCh := make(chan string, 2)
	ts := httptest.NewServer(httpTestHandler(sqlCh))
	defer ts.Close()

	oldClient := httpClient
	httpClient = ts.Client()
	defer func() { httpClient = oldClient }()

	w := &Watcher{
		sqlURL: ts.URL,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	fctx := newCodexFileContext("", nil)

	for i := 0; i < 2; i++ {
		w.processCodexLine("rollout-2026-03-27T18-10-59",
			`{"timestamp":"2026-03-27T18:11:01Z","type":"event_msg","payload":{"type":"task_started","turn_id":"turn-1"}}`,
			nil, fctx)
	}

	sql := waitForSQL(t, sqlCh)
	for _, want := range []string{"tma1_hook_events", "TaskCreated", "turn-1"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected SQL to contain %q, got %s", want, sql)
		}
	}
	assertNoSQL(t, sqlCh)
}

func TestSeedCodexSeenStatePopulatesMessageAndHookKeys(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			w.WriteHeader(500)
			return
		}
		sql := r.Form.Get("sql")
		w.WriteHeader(200)
		switch {
		case strings.Contains(sql, "FROM tma1_messages"):
			_, _ = w.Write([]byte(`{"output":[{"records":{"rows":[["assistant","assistant","","gpt-5.5",""],["tool_use","assistant","{}","gpt-5.5","call-1"]]}}]}`))
		case strings.Contains(sql, "FROM tma1_hook_events"):
			_, _ = w.Write([]byte(`{"output":[{"records":{"rows":[["PreToolUse","call-1","agent-1","review","exec_command","{}","","conv-1",""]]}}]}`))
		default:
			_, _ = w.Write([]byte(`{"output":[]}`))
		}
	}))
	defer ts.Close()

	oldClient := httpClient
	httpClient = ts.Client()
	defer func() { httpClient = oldClient }()

	w := &Watcher{
		sqlURL: ts.URL,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	seen := make(map[string]struct{})
	hookSeen := make(map[string]struct{})
	w.seedCodexSeenState("rollout-2026-03-27T18-10-59", seen, hookSeen)

	if _, ok := seen["model:gpt-5.5"]; !ok {
		t.Fatalf("expected model key to be seeded, got %#v", seen)
	}
	if _, ok := seen[codexMessageSeenKey("tool_use", "assistant", "{}", "call-1")]; !ok {
		t.Fatalf("expected tool message key to be seeded, got %#v", seen)
	}
	hookKey := codexHookKey("PreToolUse", "call-1", "agent-1", "review", "exec_command", "{}", "", "conv-1", "")
	if _, ok := hookSeen[hookKey]; !ok {
		t.Fatalf("expected hook key %q to be seeded, got %#v", hookKey, hookSeen)
	}
}

func assertAnySQLContains(t *testing.T, sqls []string, wants ...string) {
	t.Helper()
	for _, sql := range sqls {
		ok := true
		for _, want := range wants {
			if !strings.Contains(sql, want) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
	}
	t.Fatalf("expected one SQL to contain %q, got %q", wants, sqls)
}

func assertNoSQL(t *testing.T, sqlCh <-chan string) {
	t.Helper()
	select {
	case sql := <-sqlCh:
		t.Fatalf("unexpected SQL insert: %s", sql)
	case <-time.After(150 * time.Millisecond):
	}
}

func httpTestHandler(sqlCh chan<- string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			w.WriteHeader(500)
			return
		}
		sqlCh <- r.Form.Get("sql")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"output":[]}`))
	}
}

func waitForSQL(t *testing.T, sqlCh <-chan string) string {
	t.Helper()
	select {
	case sql := <-sqlCh:
		return sql
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SQL insert")
		return ""
	}
}
