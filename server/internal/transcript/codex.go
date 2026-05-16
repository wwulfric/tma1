package transcript

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	codexScanInterval = 5 * time.Second
	codexActiveAge    = 10 * time.Minute // only watch files modified within this window
)

// StartCodexScanner periodically scans ~/.codex/sessions/ for active JSONL files
// and starts watching any new ones. Codex doesn't send hooks, so we discover
// session files by polling the filesystem.
func (w *Watcher) StartCodexScanner(ctx context.Context) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		w.logger.Warn("codex scanner: cannot determine home directory", "error", err)
		return
	}
	codexDir := filepath.Join(homeDir, ".codex", "sessions")
	w.logger.Info("codex session scanner started", "path", codexDir)

	ticker := time.NewTicker(codexScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Directory may not exist yet on fresh installs; keep polling.
			if _, err := os.Stat(codexDir); err == nil {
				w.scanCodexSessions(codexDir)
			}
		}
	}
}

func (w *Watcher) scanCodexSessions(baseDir string) {
	now := time.Now()

	// Prune stopped codex watcher entries to prevent unbounded memory growth.
	// Keep recent stopped entries (their seen maps prevent re-insertion on restart).
	// Only prune when count exceeds threshold — old sessions from prior days.
	w.mu.Lock()
	var stoppedCount int
	for key, sw := range w.sessions {
		if sw.stopped && strings.HasPrefix(key, "codex:") {
			stoppedCount++
		}
	}
	if stoppedCount > 50 {
		for key, sw := range w.sessions {
			if sw.stopped && strings.HasPrefix(key, "codex:") {
				delete(w.sessions, key)
			}
		}
	}
	w.mu.Unlock()

	// Walk today's and yesterday's date dirs to find active JSONL files.
	for _, offset := range []int{0, -1} {
		d := now.AddDate(0, 0, offset)
		dir := filepath.Join(baseDir, d.Format("2006"), d.Format("01"), d.Format("02"))
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
				continue
			}
			info, err := entry.Info()
			if err != nil || now.Sub(info.ModTime()) > codexActiveAge {
				continue
			}
			// Group files from the same Codex run by extracting the timestamp prefix
			// from the filename. Format: rollout-YYYY-MM-DDTHH-MM-SS-<uuid>.jsonl
			// Files from the same run share the timestamp prefix but have different UUIDs
			// (main session vs subagent).
			baseName := strings.TrimSuffix(entry.Name(), ".jsonl")
			// watcherKey is unique per file; sessionID groups files from the same run.
			sessionID := codexSessionGroup(baseName)
			watcherKey := "codex:" + baseName
			filePath := filepath.Join(dir, entry.Name())
			w.watchCodex(watcherKey, sessionID, filePath)
		}
	}
}

// codexSessionGroup extracts the timestamp prefix from a Codex JSONL filename.
// "rollout-2026-03-27T18-10-59-019d2ec6-958f-..." → "rollout-2026-03-27T18-10-59"
// This groups main session + subagent files into one session.
func codexSessionGroup(baseName string) string {
	// Extract timestamp prefix by finding the 3rd hyphen after 'T'.
	// "rollout-2026-03-27T18-10-59-<uuid>" → "rollout-2026-03-27T18-10-59"
	tIdx := strings.IndexByte(baseName, 'T')
	if tIdx == -1 {
		return baseName
	}
	dashCount := 0
	for i := tIdx + 1; i < len(baseName); i++ {
		if baseName[i] == '-' {
			dashCount++
			if dashCount == 3 {
				return baseName[:i]
			}
		}
	}
	return baseName
}

func (w *Watcher) watchCodex(watcherKey, sessionID, filePath string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	existing, ok := w.sessions[watcherKey]
	if ok && !existing.stopped {
		return // already watching this file
	}

	// Reuse existing seen maps to avoid re-inserting previously processed lines.
	var seen map[string]struct{}
	if ok && existing.seen != nil {
		seen = existing.seen
	} else {
		seen = make(map[string]struct{})
	}
	var hookSeen map[string]struct{}
	if ok && existing.hookSeen != nil {
		hookSeen = existing.hookSeen
	} else {
		hookSeen = make(map[string]struct{})
	}

	ctx, cancel := context.WithCancel(context.Background())
	sw := &sessionWatch{cancel: cancel, seen: seen, hookSeen: hookSeen}
	w.sessions[watcherKey] = sw

	go w.tailCodexFile(ctx, watcherKey, sessionID, filePath, seen, hookSeen)
	w.logger.Info("watching codex session", "session", sessionID, "file", filePath)
}

// tailCodexFile reads a Codex JSONL session file and inserts events into GreptimeDB.
func (w *Watcher) tailCodexFile(ctx context.Context, watcherKey, sessionID, filePath string, seen, hookSeen map[string]struct{}) {
	// Mark as stopped on exit so scanner can restart with preserved seen map.
	defer func() {
		w.mu.Lock()
		if sw, ok := w.sessions[watcherKey]; ok {
			sw.stopped = true
		}
		w.mu.Unlock()
	}()
	var f *os.File
	for i := 0; i < 5; i++ {
		var err error
		f, err = os.Open(filePath) //nolint:gosec
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval):
		}
	}
	if f == nil {
		return
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	var buf strings.Builder
	fctx := newCodexFileContext(watcherKey, hookSeen) // populated by session_meta event
	w.seedCodexSeenState(sessionID, seen, fctx.hookSeen)
	idleCount := 0
	const maxIdlePolls = 600 // 5 minutes at 500ms interval
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			idleCount = 0 // reset on activity
			buf.WriteString(line)
			if strings.HasSuffix(line, "\n") {
				trimmed := strings.TrimSpace(buf.String())
				buf.Reset()
				if trimmed != "" {
					w.processCodexLine(sessionID, trimmed, seen, fctx)
				}
			}
			continue
		}
		if err == io.EOF {
			// First EOF marks end of backfill — subsequent lines are live.
			if !fctx.live {
				fctx.live = true
			}
			idleCount++
			if idleCount > maxIdlePolls {
				w.logger.Info("codex session idle, stopping watcher", "session", sessionID)
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
			}
			continue
		}
		if err != nil {
			w.logger.Debug("codex file read error", "session", sessionID, "error", err)
			return
		}
	}
}

// codexEvent represents a single line in a Codex JSONL session file.
type codexEvent struct {
	Timestamp string          `json:"timestamp"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
}

type codexResponseItem struct {
	Type    string          `json:"type"`
	Role    string          `json:"role"`
	Name    string          `json:"name"`
	CallID  string          `json:"call_id"`
	Content json.RawMessage `json:"content"`
	Summary json.RawMessage `json:"summary"`
	Output  string          `json:"output"`
	Input   string          `json:"input"`
	// function_call fields
	Arguments string `json:"arguments"`
}

// codexFileContext tracks per-file agent identity (main vs subagent).
type codexFileContext struct {
	fileID         string
	agentID        string
	agentType      string
	conversationID string // from session_meta.payload.id (= OTel conversation.id)
	model          string
	hookSeen       map[string]struct{}
	live           bool // true after initial backfill completes (first EOF)
}

func newCodexFileContext(fileID string, hookSeen map[string]struct{}) *codexFileContext {
	if hookSeen == nil {
		hookSeen = make(map[string]struct{})
	}
	return &codexFileContext{fileID: fileID, hookSeen: hookSeen}
}

func (w *Watcher) processCodexLine(sessionID, line string, seen map[string]struct{}, fctx *codexFileContext) {
	var ev codexEvent
	if err := json.Unmarshal([]byte(line), &ev); err != nil {
		return
	}

	// Parse timestamp from event.
	ts, _ := time.Parse(time.RFC3339Nano, ev.Timestamp)
	if ts.IsZero() {
		ts = time.Now()
	}

	if ev.Type == "session_meta" {
		w.applyCodexSessionMeta(ev.Payload, fctx)
	}
	if ev.Type == "turn_context" {
		w.applyCodexTurnContext(ev.Payload, fctx)
	}

	switch ev.Type {
	case "session_meta":
		// Detect subagent from source field: {"subagent": "review"} vs "cli"
		meta := parseCodexSessionMeta(ev.Payload)
		if meta.subagent != "" {
			if fctx != nil {
				w.insertCodexSubagentEvent(sessionID, ts, fctx.agentID, fctx.agentType, fctx.conversationID, fctx)
				if fctx.live {
					w.broadcastHookEvent(sessionID, "SubagentStart", "", "", "", "", fctx.agentID, fctx.agentType)
				}
			}
			break
		}
		conversationID := ""
		if fctx != nil {
			conversationID = fctx.conversationID
		}
		w.insertCodexSessionStart(sessionID, ts, meta.cwd, conversationID, fctx)
		if fctx != nil && fctx.live {
			w.broadcastHookEvent(sessionID, "SessionStart", "", "", "", "", "", "")
		}

	case "turn_context":
		// Extract model name and store as a message with model field set.
		if fctx != nil && fctx.model != "" {
			w.insertCodexModelMessage(sessionID, ts, fctx.model, seen)
		}

	case "event_msg":
		var eventMsg struct {
			Type    string          `json:"type"`
			Message string          `json:"message"`
			Phase   string          `json:"phase"`
			TurnID  string          `json:"turn_id"`
			CallID  string          `json:"call_id"`
			Query   string          `json:"query"`
			Action  json.RawMessage `json:"action"`
		}
		if err := json.Unmarshal(ev.Payload, &eventMsg); err != nil {
			return
		}
		switch eventMsg.Type {
		case "task_started":
			w.insertCodexHookEvent(sessionID, ts, "TaskCreated", "", "", eventMsg.TurnID, "", fctx)
		case "task_complete":
			w.insertCodexHookEvent(sessionID, ts, "TaskCompleted", "", "", eventMsg.TurnID, "", fctx)
			// Emit SubagentStop for subagent files.
			if fctx != nil && fctx.agentID != "" {
				w.insertCodexHookEvent(sessionID, ts, "SubagentStop", "", "", "", "", fctx)
			}
		case "user_message":
			msg := strings.TrimSpace(eventMsg.Message)
			if msg != "" {
				w.insertCodexMessage(sessionID, ts, "user", msg, seen, fctx)
			}
		case "agent_message":
			msg := strings.TrimSpace(eventMsg.Message)
			if msg != "" {
				w.insertCodexMessage(sessionID, ts, "assistant", msg, seen, fctx)
			}
		case "web_search_end":
			toolInput := codexWebSearchInput(eventMsg.Query, eventMsg.Action)
			w.insertCodexHookEvent(sessionID, ts, "PreToolUse", "web_search", toolInput, eventMsg.CallID, "", fctx)
			w.insertCodexTypedMessage(sessionID, ts, "tool_use", "assistant", toolInput, codexModel(fctx), "web_search", eventMsg.CallID, seen)
			w.insertCodexHookEvent(sessionID, ts, "PostToolUse", "web_search", "", eventMsg.CallID, toolInput, fctx)
			w.insertCodexTypedMessage(sessionID, ts, "tool_result", "user", toolInput, codexModel(fctx), "web_search", eventMsg.CallID, seen)
		}

	case "response_item":
		var item codexResponseItem
		if err := json.Unmarshal(ev.Payload, &item); err != nil {
			return
		}
		w.processCodexResponseItem(sessionID, ts, item, seen, fctx)
	}
}

func (w *Watcher) processCodexResponseItem(sessionID string, ts time.Time, item codexResponseItem, seen map[string]struct{}, fctx *codexFileContext) {
	switch item.Type {
	case "message":
		role := item.Role
		if role == "developer" {
			return // system/developer messages not relevant
		}
		// Extract text content.
		var contentBlocks []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(item.Content, &contentBlocks); err != nil {
			// Try as single string.
			var s string
			if err := json.Unmarshal(item.Content, &s); err == nil && s != "" {
				w.insertCodexMessage(sessionID, ts, role, s, seen, fctx)
			}
			return
		}
		for _, b := range contentBlocks {
			if (b.Type == "input_text" || b.Type == "output_text" || b.Type == "text") && b.Text != "" {
				w.insertCodexMessage(sessionID, ts, role, b.Text, seen, fctx)
			}
		}

	case "reasoning":
		if text := extractCodexReasoning(item); text != "" {
			w.insertCodexTypedMessage(sessionID, ts, "thinking", "assistant", text, codexModel(fctx), "", "", seen)
		}

	case "function_call":
		w.insertCodexHookEvent(sessionID, ts, "PreToolUse", item.Name, item.Arguments, item.CallID, "", fctx)
		w.insertCodexTypedMessage(sessionID, ts, "tool_use", "assistant", item.Arguments, codexModel(fctx), item.Name, item.CallID, seen)

	case "function_call_output":
		w.insertCodexHookEvent(sessionID, ts, "PostToolUse", "", "", item.CallID, item.Output, fctx)
		w.insertCodexTypedMessage(sessionID, ts, "tool_result", "user", item.Output, codexModel(fctx), "", item.CallID, seen)

	case "custom_tool_call":
		w.insertCodexHookEvent(sessionID, ts, "PreToolUse", item.Name, item.Input, item.CallID, "", fctx)
		w.insertCodexTypedMessage(sessionID, ts, "tool_use", "assistant", item.Input, codexModel(fctx), item.Name, item.CallID, seen)

	case "custom_tool_call_output":
		w.insertCodexHookEvent(sessionID, ts, "PostToolUse", "", "", item.CallID, item.Output, fctx)
		w.insertCodexTypedMessage(sessionID, ts, "tool_result", "user", item.Output, codexModel(fctx), "", item.CallID, seen)
	}
}

type codexSessionMeta struct {
	id       string
	cwd      string
	subagent string
}

func parseCodexSessionMeta(raw json.RawMessage) codexSessionMeta {
	var meta struct {
		ID     string          `json:"id"`
		Source json.RawMessage `json:"source"`
		CWD    string          `json:"cwd"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return codexSessionMeta{}
	}
	out := codexSessionMeta{id: meta.ID, cwd: meta.CWD}
	out.subagent = parseCodexSubagentSource(meta.Source)
	return out
}

func parseCodexSubagentSource(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var sourceString string
	if json.Unmarshal(raw, &sourceString) == nil {
		return ""
	}
	var source struct {
		Subagent json.RawMessage `json:"subagent"`
	}
	if json.Unmarshal(raw, &source) != nil || len(source.Subagent) == 0 || string(source.Subagent) == "null" {
		return ""
	}
	var subagent string
	if json.Unmarshal(source.Subagent, &subagent) == nil {
		return strings.TrimSpace(subagent)
	}
	var subagentObj map[string]string
	if json.Unmarshal(source.Subagent, &subagentObj) == nil {
		if other := strings.TrimSpace(subagentObj["other"]); other != "" {
			if other == "guardian" {
				return "codex-auto-review"
			}
			return other
		}
	}
	return ""
}

func (w *Watcher) applyCodexSessionMeta(raw json.RawMessage, fctx *codexFileContext) {
	if fctx == nil {
		return
	}
	meta := parseCodexSessionMeta(raw)
	if meta.id != "" {
		fctx.conversationID = meta.id
	}
	if meta.subagent != "" {
		fctx.agentID = codexSubagentID(fctx.fileID, meta.subagent)
		fctx.agentType = meta.subagent
	}
}

func (w *Watcher) applyCodexTurnContext(raw json.RawMessage, fctx *codexFileContext) {
	if fctx == nil {
		return
	}
	var turnCtx struct {
		Model string `json:"model"`
	}
	if json.Unmarshal(raw, &turnCtx) == nil && turnCtx.Model != "" {
		fctx.model = turnCtx.Model
	}
}

func extractCodexReasoning(item codexResponseItem) string {
	if text := extractCodexText(item.Content, "reasoning_text", "text", "output_text"); text != "" {
		return text
	}
	return extractCodexText(item.Summary, "summary_text", "text")
}

func extractCodexText(raw json.RawMessage, allowedTypes ...string) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return strings.TrimSpace(s)
	}
	allowed := make(map[string]struct{}, len(allowedTypes))
	for _, typ := range allowedTypes {
		allowed[typ] = struct{}{}
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return ""
	}
	var sb strings.Builder
	for _, b := range blocks {
		if _, ok := allowed[b.Type]; !ok {
			continue
		}
		text := strings.TrimSpace(b.Text)
		if text == "" {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(text)
	}
	return sb.String()
}

func codexWebSearchInput(query string, action json.RawMessage) string {
	query = strings.TrimSpace(query)
	actionText := strings.TrimSpace(string(action))
	if query == "" {
		if actionText == "null" {
			return ""
		}
		return actionText
	}
	if actionText == "" || actionText == "null" {
		return query
	}
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return actionText
	}
	return `{"query":` + string(queryJSON) + `,"action":` + actionText + `}`
}

func codexModel(fctx *codexFileContext) string {
	if fctx == nil {
		return ""
	}
	return fctx.model
}

func (fctx *codexFileContext) seenCodexHook(eventType, toolUseID, agentID, agentType, toolName, toolInput, toolResult, conversationID, cwd string) bool {
	if fctx == nil || fctx.hookSeen == nil {
		return false
	}
	key := codexHookKey(eventType, toolUseID, agentID, agentType, toolName, toolInput, toolResult, conversationID, cwd)
	if _, ok := fctx.hookSeen[key]; ok {
		return true
	}
	fctx.hookSeen[key] = struct{}{}
	return false
}

func codexHookKey(eventType, toolUseID, agentID, agentType, toolName, toolInput, toolResult, conversationID, cwd string) string {
	if toolUseID != "" {
		return eventType + ":" + toolUseID
	}
	switch eventType {
	case "SessionStart":
		return eventType + ":" + conversationID + ":" + cwd
	case "SubagentStart", "SubagentStop":
		return eventType + ":" + agentID + ":" + agentType + ":" + conversationID
	}
	prefix := toolInput
	if prefix == "" {
		prefix = toolResult
	}
	if len(prefix) > 200 {
		prefix = prefix[:200]
	}
	return eventType + ":" + agentID + ":" + agentType + ":" + toolName + ":" + prefix
}

func codexMessageSeenKey(messageType, role, content, toolUseID string) string {
	prefix := content
	if len(prefix) > 200 {
		prefix = prefix[:200]
	}
	if toolUseID != "" && (messageType == "tool_use" || messageType == "tool_result" || messageType == "llm") {
		return messageType + ":" + toolUseID
	}
	return messageType + ":" + role + ":" + prefix
}

// insertCodexModelMessage stores a synthetic message with the model field set.
// This makes the model visible in session detail KPI and cost calculation.
func (w *Watcher) insertCodexModelMessage(sessionID string, ts time.Time, model string, seen map[string]struct{}) {
	key := "model:" + model
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}

	msTs := ts.UnixMilli()
	for {
		prev := lastInsertTS.Load()
		next := msTs
		if next <= prev {
			next = prev + 1
		}
		if lastInsertTS.CompareAndSwap(prev, next) {
			msTs = next
			break
		}
	}

	sql := fmt.Sprintf(
		"INSERT INTO tma1_messages (ts, session_id, message_type, \"role\", content, model, tool_name, tool_use_id) "+
			"VALUES (%d, '%s', 'assistant', 'assistant', '', '%s', '', '')",
		msTs,
		escapeSQLString(sessionID),
		escapeSQLString(model),
	)
	go func() {
		insertSem <- struct{}{}
		defer func() { <-insertSem }()
		w.execSQL(sql)
	}()

	// Do NOT broadcast model messages — they are synthetic metadata, not hook events.
}

func (w *Watcher) insertCodexMessage(sessionID string, ts time.Time, role, content string, seen map[string]struct{}, fctx *codexFileContext) {
	msgType := "user"
	if role == "assistant" {
		msgType = "assistant"
	}
	w.insertCodexTypedMessage(sessionID, ts, msgType, role, content, codexModel(fctx), "", "", seen)
}

func (w *Watcher) insertCodexTypedMessage(sessionID string, ts time.Time, messageType, role, content, model, toolName, toolUseID string, seen map[string]struct{}) {
	// Dedup by stable identity. Tool messages use call id; text messages use a
	// content prefix so Codex context replays do not inflate conversation rows.
	key := codexMessageSeenKey(messageType, role, content, toolUseID)
	if seen != nil {
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
	}

	msTs := ts.UnixMilli()
	for {
		prev := lastInsertTS.Load()
		next := msTs
		if next <= prev {
			next = prev + 1
		}
		if lastInsertTS.CompareAndSwap(prev, next) {
			msTs = next
			break
		}
	}

	sql := fmt.Sprintf(
		"INSERT INTO tma1_messages (ts, session_id, message_type, \"role\", content, model, tool_name, tool_use_id) "+
			"VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s', '%s')",
		msTs,
		escapeSQLString(sessionID),
		escapeSQLString(messageType),
		escapeSQLString(role),
		escapeSQLString(truncate(content, maxContentLen)),
		escapeSQLString(model),
		escapeSQLString(toolName),
		escapeSQLString(toolUseID),
	)
	go func() {
		insertSem <- struct{}{}
		defer func() { <-insertSem }()
		w.execSQL(sql)
	}()
}

func (w *Watcher) seedCodexSeenState(sessionID string, seen, hookSeen map[string]struct{}) {
	if seen != nil {
		messageSQL := "SELECT message_type, \"role\", content, model, tool_use_id " +
			"FROM tma1_messages WHERE session_id = '" + escapeSQLString(sessionID) + "' LIMIT 100000"
		for _, row := range w.fetchCodexRows(messageSQL, sessionID, "messages") {
			if len(row) < 5 {
				continue
			}
			messageType := sqlRowString(row[0])
			role := sqlRowString(row[1])
			content := sqlRowString(row[2])
			model := sqlRowString(row[3])
			toolUseID := sqlRowString(row[4])
			seen[codexMessageSeenKey(messageType, role, content, toolUseID)] = struct{}{}
			if messageType == "assistant" && role == "assistant" && content == "" && model != "" {
				seen["model:"+model] = struct{}{}
			}
		}
	}
	if hookSeen != nil {
		hookSQL := "SELECT event_type, tool_use_id, agent_id, agent_type, tool_name, tool_input, " +
			"tool_result, conversation_id, cwd FROM tma1_hook_events WHERE session_id = '" +
			escapeSQLString(sessionID) + "' AND agent_source = 'codex' LIMIT 100000"
		for _, row := range w.fetchCodexRows(hookSQL, sessionID, "hooks") {
			if len(row) < 9 {
				continue
			}
			eventType := sqlRowString(row[0])
			toolUseID := sqlRowString(row[1])
			agentID := sqlRowString(row[2])
			agentType := sqlRowString(row[3])
			toolName := sqlRowString(row[4])
			toolInput := sqlRowString(row[5])
			toolResult := sqlRowString(row[6])
			conversationID := sqlRowString(row[7])
			cwd := sqlRowString(row[8])
			hookSeen[codexHookKey(eventType, toolUseID, agentID, agentType, toolName, toolInput, toolResult, conversationID, cwd)] = struct{}{}
		}
	}
}

func (w *Watcher) fetchCodexRows(sql, sessionID, table string) [][]json.RawMessage {
	form := url.Values{}
	form.Set("sql", sql)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := newPostRequest(ctx, w.sqlURL, form)
	if err != nil {
		w.logger.Debug("codex: failed to build seen-state query", "session", sessionID, "table", table, "error", err)
		return nil
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		w.logger.Debug("codex: failed to fetch seen state", "session", sessionID, "table", table, "error", err)
		return nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 200 {
		w.logger.Debug("codex: failed to read seen state", "session", sessionID, "table", table, "status", resp.StatusCode, "error", err)
		return nil
	}
	var output struct {
		Output []struct {
			Records struct {
				Rows [][]json.RawMessage `json:"rows"`
			} `json:"records"`
		} `json:"output"`
	}
	if json.Unmarshal(body, &output) != nil {
		w.logger.Debug("codex: failed to parse seen state", "session", sessionID, "table", table)
		return nil
	}
	if len(output.Output) == 0 {
		return nil
	}
	return output.Output[0].Records.Rows
}

func sqlRowString(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return strings.Trim(string(raw), `"`)
}

func (w *Watcher) insertCodexSessionStart(sessionID string, ts time.Time, cwd, conversationID string, fctx *codexFileContext) {
	if fctx != nil && fctx.seenCodexHook("SessionStart", "", "", "", "", "", "", conversationID, cwd) {
		return
	}
	msTs := ts.UnixMilli()
	for {
		prev := lastInsertTS.Load()
		next := msTs
		if next <= prev {
			next = prev + 1
		}
		if lastInsertTS.CompareAndSwap(prev, next) {
			msTs = next
			break
		}
	}

	sql := fmt.Sprintf(
		"INSERT INTO tma1_hook_events "+
			"(ts, session_id, event_type, agent_source, tool_name, tool_input, tool_result, "+
			"tool_use_id, agent_id, agent_type, notification_type, \"message\", cwd, transcript_path, conversation_id) "+
			"VALUES (%d, '%s', 'SessionStart', 'codex', '', '', '', '', '', '', '', '', '%s', '', '%s')",
		msTs,
		escapeSQLString(sessionID),
		escapeSQLString(truncate(cwd, 512)),
		escapeSQLString(conversationID),
	)
	go func() {
		insertSem <- struct{}{}
		defer func() { <-insertSem }()
		w.execSQL(sql)
	}()
}

func codexSubagentID(fileID, agentType string) string {
	if fileID != "" {
		return fileID
	}
	return agentType
}

func (w *Watcher) insertCodexSubagentEvent(sessionID string, ts time.Time, agentID, agentType, conversationID string, fctx *codexFileContext) {
	if fctx != nil && fctx.seenCodexHook("SubagentStart", "", agentID, agentType, "", "", "", conversationID, "") {
		return
	}
	msTs := ts.UnixMilli()
	for {
		prev := lastInsertTS.Load()
		next := msTs
		if next <= prev {
			next = prev + 1
		}
		if lastInsertTS.CompareAndSwap(prev, next) {
			msTs = next
			break
		}
	}

	sql := fmt.Sprintf(
		"INSERT INTO tma1_hook_events "+
			"(ts, session_id, event_type, agent_source, tool_name, tool_input, tool_result, "+
			"tool_use_id, agent_id, agent_type, notification_type, \"message\", cwd, transcript_path, conversation_id) "+
			"VALUES (%d, '%s', 'SubagentStart', 'codex', '', '', '', '', '%s', '%s', '', '', '', '', '%s')",
		msTs,
		escapeSQLString(sessionID),
		escapeSQLString(agentID),
		escapeSQLString(agentType),
		escapeSQLString(conversationID),
	)
	go func() {
		insertSem <- struct{}{}
		defer func() { <-insertSem }()
		w.execSQL(sql)
	}()
}

func (w *Watcher) insertCodexHookEvent(sessionID string, ts time.Time, eventType, toolName, toolInput, toolUseID, toolResult string, fctx *codexFileContext) {
	agentID := ""
	agentType := ""
	if fctx != nil {
		agentID = fctx.agentID
		agentType = fctx.agentType
	}

	conversationID := ""
	if fctx != nil {
		conversationID = fctx.conversationID
	}
	if fctx != nil && fctx.seenCodexHook(eventType, toolUseID, agentID, agentType, toolName, toolInput, toolResult, conversationID, "") {
		return
	}

	msTs := ts.UnixMilli()
	for {
		prev := lastInsertTS.Load()
		next := msTs
		if next <= prev {
			next = prev + 1
		}
		if lastInsertTS.CompareAndSwap(prev, next) {
			msTs = next
			break
		}
	}

	sql := fmt.Sprintf(
		"INSERT INTO tma1_hook_events "+
			"(ts, session_id, event_type, agent_source, tool_name, tool_input, tool_result, "+
			"tool_use_id, agent_id, agent_type, notification_type, \"message\", cwd, transcript_path, conversation_id) "+
			"VALUES (%d, '%s', '%s', 'codex', '%s', '%s', '%s', '%s', '%s', '%s', '', '', '', '', '%s')",
		msTs,
		escapeSQLString(sessionID),
		escapeSQLString(eventType),
		escapeSQLString(truncate(toolName, 256)),
		escapeSQLString(truncate(toolInput, maxToolInput)),
		escapeSQLString(truncate(toolResult, maxToolContent)),
		escapeSQLString(toolUseID),
		escapeSQLString(agentID),
		escapeSQLString(agentType),
		escapeSQLString(conversationID),
	)
	go func() {
		insertSem <- struct{}{}
		defer func() { <-insertSem }()
		w.execSQL(sql)
	}()

	if fctx != nil && fctx.live {
		w.broadcastHookEvent(sessionID, eventType, toolName, toolInput, toolUseID, toolResult, agentID, agentType)
	}
}
