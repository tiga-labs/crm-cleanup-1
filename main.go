package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	_ "embed"
)

//go:embed index.html
var indexHTML []byte

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const (
	sfAPIVersion      = "v59.0"
	csvLogFile        = "crm_ops_log.csv"
	scanWorkers       = 5
	sfBatchSize       = 25
	dupScoreThreshold = 40
)

// ---------------------------------------------------------------------------
// CSV logger
// ---------------------------------------------------------------------------

var csvMu sync.Mutex

func initCSVLog() {
	csvMu.Lock()
	defer csvMu.Unlock()
	if _, err := os.Stat(csvLogFile); err == nil {
		return
	}
	f, err := os.Create(csvLogFile)
	if err != nil {
		log.Printf("Failed to create CSV log: %v", err)
		return
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"timestamp", "operation", "crm_id", "old_value", "new_value", "error"})
	w.Flush()
}

func logCSV(operation, crmID, oldVal, newVal, errMsg string) {
	csvMu.Lock()
	defer csvMu.Unlock()
	f, err := os.OpenFile(csvLogFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{time.Now().UTC().Format(time.RFC3339), operation, crmID, oldVal, newVal, errMsg})
	w.Flush()
}

func readProcessedIDs(operation string) map[string]bool {
	csvMu.Lock()
	defer csvMu.Unlock()
	ids := make(map[string]bool)
	f, err := os.Open(csvLogFile)
	if err != nil {
		return ids
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return ids
	}
	for _, row := range records[1:] {
		if len(row) >= 3 && row[1] == operation {
			ids[row[2]] = true
		}
	}
	return ids
}

// ---------------------------------------------------------------------------
// Logged HTTP helper
// ---------------------------------------------------------------------------

func apiDo(method, rawURL string, body io.Reader, headers map[string]string) ([]byte, int, error) {
	log.Printf("[API] %s %s", method, rawURL)
	req, err := http.NewRequest(method, rawURL, body)
	if err != nil {
		return nil, 0, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[API] %s %s -> ERROR: %v", method, rawURL, err)
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	log.Printf("[API] %s %s -> %d (%d bytes)", method, rawURL, resp.StatusCode, len(data))
	return data, resp.StatusCode, nil
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type AppStatus string

const (
	StatusIdle         AppStatus = "idle"
	StatusScanning     AppStatus = "scanning"
	StatusScanned      AppStatus = "scanned"
	StatusApplying     AppStatus = "applying"
	StatusApplied      AppStatus = "applied"
	StatusUndoing      AppStatus = "undoing"
	StatusStopped      AppStatus = "stopped"
	StatusScanningDups AppStatus = "scanning_dupes"
	StatusScannedDups  AppStatus = "scanned_dupes"
	StatusApplyingDups AppStatus = "applying_dupes"
	StatusAppliedDups  AppStatus = "applied_dupes"
)

type ChangeEntry struct {
	ID       string `json:"id"`
	OldName  string `json:"old_name"`
	NewName  string `json:"new_name"`
	Selected bool   `json:"selected"`
}

type SFAccount struct {
	ID           string `json:"Id"`
	Name         string `json:"Name"`
	Website      string `json:"Website"`
	Phone        string `json:"Phone"`
	BillingCity  string `json:"BillingCity"`
	BillingState string `json:"BillingState"`
	CreatedDate  string `json:"CreatedDate"`
}

type DupeGroup struct {
	MasterID     string      `json:"master_id"`
	Accounts     []SFAccount `json:"accounts"`
	Score        int         `json:"score"`
	MatchReasons []string    `json:"match_reasons"`
}

type AppState struct {
	mu     sync.Mutex
	cancel context.CancelFunc

	// Config (from browser)
	TigaKey     string `json:"-"`
	TigaBaseURL string `json:"tiga_base_url"`
	ConfigSet   bool   `json:"config_set"`

	// Name cleaning state
	Status       AppStatus     `json:"status"`
	Message      string        `json:"message"`
	TotalRecords int           `json:"total_records"`
	Processed    int           `json:"processed"`
	Changes      []ChangeEntry `json:"changes"`
	Applied      []ChangeEntry `json:"applied"`
	ScanError    string        `json:"scan_error,omitempty"`
	ApplyErrors  []string      `json:"apply_errors,omitempty"`
	LastOp       string        `json:"last_op,omitempty"`

	// Dedup state
	DupeGroups  []DupeGroup `json:"dupe_groups"`
	DupeApplied []DupeGroup `json:"dupe_applied"`

	// Cached accounts
	cachedAccounts []SFAccount
}

func (s *AppState) set(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn()
}

func (s *AppState) tigaHeaders() map[string]string {
	return map[string]string{
		"X-Tiga-Auth":  s.TigaKey,
		"Content-Type": "application/json",
	}
}

var state = &AppState{Status: StatusIdle, Message: "Configure your Tiga API key to get started."}

// ---------------------------------------------------------------------------
// Tiga + Salesforce API helpers
// ---------------------------------------------------------------------------

type sfToken struct {
	AccessToken string `json:"access_token"`
	InstanceURL string `json:"instance_url"`
}

func getSFToken(tigaKey, tigaBaseURL string) (sfToken, error) {
	headers := map[string]string{
		"X-Tiga-Auth":  tigaKey,
		"Content-Type": "application/json",
	}
	data, code, err := apiDo("GET", tigaBaseURL+"/api/v1/current-org/salesforce-oauth-token", nil, headers)
	if err != nil {
		return sfToken{}, err
	}
	if code != 200 {
		return sfToken{}, fmt.Errorf("tiga returned %d: %s", code, string(data))
	}
	var tok sfToken
	if err := json.Unmarshal(data, &tok); err != nil {
		return sfToken{}, err
	}
	tok.InstanceURL = strings.TrimRight(tok.InstanceURL, "/")
	return tok, nil
}

type sfQueryResponse struct {
	TotalSize      int         `json:"totalSize"`
	Done           bool        `json:"done"`
	NextRecordsURL string      `json:"nextRecordsUrl"`
	Records        []SFAccount `json:"records"`
}

func queryAllAccounts(tok sfToken) ([]SFAccount, error) {
	var all []SFAccount
	soql := "SELECT+Id,Name,Website,Phone,BillingCity,BillingState,CreatedDate+FROM+Account+ORDER+BY+Name+ASC"
	rawURL := tok.InstanceURL + "/services/data/" + sfAPIVersion + "/query?q=" + soql

	headers := map[string]string{
		"Authorization": "Bearer " + tok.AccessToken,
		"Content-Type":  "application/json",
	}

	for {
		data, _, err := apiDo("GET", rawURL, nil, headers)
		if err != nil {
			return nil, err
		}
		var resp sfQueryResponse
		if err := json.Unmarshal(data, &resp); err != nil {
			return nil, err
		}
		all = append(all, resp.Records...)
		if resp.Done {
			break
		}
		rawURL = tok.InstanceURL + resp.NextRecordsURL
	}
	return all, nil
}

type cleanNameResponse struct {
	CleanedAccountName string `json:"cleaned_account_name"`
}

func cleanAccountName(tigaKey, tigaBaseURL, name string) (string, error) {
	headers := map[string]string{
		"X-Tiga-Auth":  tigaKey,
		"Content-Type": "application/json",
	}
	body, _ := json.Marshal(map[string]string{"account_name": name})
	data, code, err := apiDo("POST", tigaBaseURL+"/api/v1/util/clean-account-name",
		bytes.NewReader(body), headers)
	if err != nil {
		return "", err
	}
	if code != 200 {
		return "", fmt.Errorf("clean-account-name returned %d: %s", code, string(data))
	}
	var resp cleanNameResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return "", err
	}
	return resp.CleanedAccountName, nil
}

// ---------------------------------------------------------------------------
// Salesforce Composite batch update
// ---------------------------------------------------------------------------

type compositeSubrequest struct {
	Method      string            `json:"method"`
	URL         string            `json:"url"`
	ReferenceID string            `json:"referenceId"`
	Body        map[string]string `json:"body,omitempty"`
}

type compositeRequest struct {
	AllOrNone        bool                  `json:"allOrNone"`
	CompositeRequest []compositeSubrequest `json:"compositeRequest"`
}

type compositeSubresponse struct {
	ReferenceID    string          `json:"referenceId"`
	HTTPStatusCode int             `json:"httpStatusCode"`
	Body           json.RawMessage `json:"body"`
}

type compositeResponse struct {
	CompositeResponse []compositeSubresponse `json:"compositeResponse"`
}

func applyBatch(tok sfToken, batch []ChangeEntry) []string {
	var subreqs []compositeSubrequest
	for i, ch := range batch {
		subreqs = append(subreqs, compositeSubrequest{
			Method:      "PATCH",
			URL:         "/services/data/" + sfAPIVersion + "/sobjects/Account/" + ch.ID,
			ReferenceID: fmt.Sprintf("ref%d", i),
			Body:        map[string]string{"Name": ch.NewName},
		})
	}

	reqBody, _ := json.Marshal(compositeRequest{
		AllOrNone:        false,
		CompositeRequest: subreqs,
	})

	headers := map[string]string{
		"Authorization": "Bearer " + tok.AccessToken,
		"Content-Type":  "application/json",
	}

	data, _, err := apiDo("POST",
		tok.InstanceURL+"/services/data/"+sfAPIVersion+"/composite",
		bytes.NewReader(reqBody), headers)
	if err != nil {
		var errs []string
		for _, ch := range batch {
			errs = append(errs, ch.ID+": "+err.Error())
		}
		return errs
	}

	var cr compositeResponse
	_ = json.Unmarshal(data, &cr)

	var errs []string
	for i, sub := range cr.CompositeResponse {
		if sub.HTTPStatusCode < 200 || sub.HTTPStatusCode >= 300 {
			id := ""
			if i < len(batch) {
				id = batch[i].ID
			}
			errs = append(errs, fmt.Sprintf("%s: HTTP %d %s", id, sub.HTTPStatusCode, string(sub.Body)))
		}
	}
	return errs
}

// ---------------------------------------------------------------------------
// Deduplication algorithm
// ---------------------------------------------------------------------------

var suffixPattern = regexp.MustCompile(`(?i)\b(inc|incorporated|llc|l\.l\.c|corp|corporation|ltd|limited|co|company|lp|llp|plc|gmbh|sa|sas|ag|pty|pvt|nv|bv)\.?\b`)
var punctPattern = regexp.MustCompile(`[^\w\s]`)
var spacePattern = regexp.MustCompile(`\s+`)

func normalizeName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = suffixPattern.ReplaceAllString(s, "")
	s = punctPattern.ReplaceAllString(s, "")
	s = spacePattern.ReplaceAllString(strings.TrimSpace(s), " ")
	return s
}

func normalizeDomain(website string) string {
	if website == "" {
		return ""
	}
	website = strings.TrimSpace(website)
	if !strings.Contains(website, "://") {
		website = "https://" + website
	}
	u, err := url.Parse(website)
	if err != nil {
		return ""
	}
	host := strings.ToLower(u.Hostname())
	host = strings.TrimPrefix(host, "www.")
	return host
}

func normalizePhone(phone string) string {
	var digits []byte
	for _, c := range phone {
		if c >= '0' && c <= '9' {
			digits = append(digits, byte(c))
		}
	}
	return string(digits)
}

// Jaro-Winkler similarity (0.0 to 1.0)
func jaroWinkler(s1, s2 string) float64 {
	if s1 == s2 {
		return 1.0
	}
	len1 := len(s1)
	len2 := len(s2)
	if len1 == 0 || len2 == 0 {
		return 0.0
	}

	matchDist := 0
	if len1 > len2 {
		matchDist = len1/2 - 1
	} else {
		matchDist = len2/2 - 1
	}
	if matchDist < 0 {
		matchDist = 0
	}

	s1Matches := make([]bool, len1)
	s2Matches := make([]bool, len2)

	matches := 0
	transpositions := 0

	for i := 0; i < len1; i++ {
		start := i - matchDist
		if start < 0 {
			start = 0
		}
		end := i + matchDist + 1
		if end > len2 {
			end = len2
		}
		for j := start; j < end; j++ {
			if s2Matches[j] || s1[i] != s2[j] {
				continue
			}
			s1Matches[i] = true
			s2Matches[j] = true
			matches++
			break
		}
	}

	if matches == 0 {
		return 0.0
	}

	k := 0
	for i := 0; i < len1; i++ {
		if !s1Matches[i] {
			continue
		}
		for !s2Matches[k] {
			k++
		}
		if s1[i] != s2[k] {
			transpositions++
		}
		k++
	}

	jaro := (float64(matches)/float64(len1) +
		float64(matches)/float64(len2) +
		float64(matches-transpositions/2)/float64(matches)) / 3.0

	// Winkler prefix bonus (up to 4 chars)
	prefix := 0
	for i := 0; i < int(math.Min(float64(len1), math.Min(float64(len2), 4))); i++ {
		if s1[i] == s2[i] {
			prefix++
		} else {
			break
		}
	}

	return jaro + float64(prefix)*0.1*(1.0-jaro)
}

func countPopulated(a SFAccount) int {
	count := 0
	if a.Name != "" {
		count++
	}
	if a.Website != "" {
		count++
	}
	if a.Phone != "" {
		count++
	}
	if a.BillingCity != "" {
		count++
	}
	if a.BillingState != "" {
		count++
	}
	return count
}

func computeDupeScore(a, b SFAccount) (int, []string) {
	score := 0
	var reasons []string

	// Domain match (+40)
	da := normalizeDomain(a.Website)
	db := normalizeDomain(b.Website)
	if da != "" && db != "" && da == db {
		score += 40
		reasons = append(reasons, fmt.Sprintf("Domain match: %s", da))
	}

	// Phone match (+25)
	pa := normalizePhone(a.Phone)
	pb := normalizePhone(b.Phone)
	if pa != "" && pb != "" && pa == pb {
		score += 25
		reasons = append(reasons, fmt.Sprintf("Phone match: %s", pa))
	}

	// Name similarity (+25)
	na := normalizeName(a.Name)
	nb := normalizeName(b.Name)
	if na != "" && nb != "" {
		sim := jaroWinkler(na, nb)
		if sim > 0.85 {
			score += 25
			reasons = append(reasons, fmt.Sprintf("Name similarity: %.0f%% (%s ↔ %s)", sim*100, a.Name, b.Name))
		}
	}

	// City+State match (+10)
	ca := strings.ToLower(strings.TrimSpace(a.BillingCity))
	cb := strings.ToLower(strings.TrimSpace(b.BillingCity))
	sa := strings.ToLower(strings.TrimSpace(a.BillingState))
	sb := strings.ToLower(strings.TrimSpace(b.BillingState))
	if ca != "" && cb != "" && sa != "" && sb != "" && ca == cb && sa == sb {
		score += 10
		reasons = append(reasons, fmt.Sprintf("Location match: %s, %s", a.BillingCity, a.BillingState))
	}

	return score, reasons
}

// Union-Find for grouping duplicates
type unionFind struct {
	parent []int
	rank   []int
}

func newUnionFind(n int) *unionFind {
	parent := make([]int, n)
	rank := make([]int, n)
	for i := range parent {
		parent[i] = i
	}
	return &unionFind{parent: parent, rank: rank}
}

func (uf *unionFind) find(x int) int {
	if uf.parent[x] != x {
		uf.parent[x] = uf.find(uf.parent[x])
	}
	return uf.parent[x]
}

func (uf *unionFind) union(x, y int) {
	rx, ry := uf.find(x), uf.find(y)
	if rx == ry {
		return
	}
	if uf.rank[rx] < uf.rank[ry] {
		rx, ry = ry, rx
	}
	uf.parent[ry] = rx
	if uf.rank[rx] == uf.rank[ry] {
		uf.rank[rx]++
	}
}

type scoredPair struct {
	i, j    int
	score   int
	reasons []string
}

func findDuplicateGroups(accounts []SFAccount) []DupeGroup {
	n := len(accounts)
	if n < 2 {
		return nil
	}

	uf := newUnionFind(n)
	pairScores := make(map[[2]int]scoredPair)

	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			score, reasons := computeDupeScore(accounts[i], accounts[j])
			if score >= dupScoreThreshold {
				uf.union(i, j)
				pairScores[[2]int{i, j}] = scoredPair{i: i, j: j, score: score, reasons: reasons}
			}
		}
	}

	// Collect groups
	groups := make(map[int][]int)
	for i := 0; i < n; i++ {
		root := uf.find(i)
		groups[root] = append(groups[root], i)
	}

	var result []DupeGroup
	for _, members := range groups {
		if len(members) < 2 {
			continue
		}

		// Collect all reasons and max score for the group
		maxScore := 0
		var allReasons []string
		reasonSet := make(map[string]bool)
		for a := 0; a < len(members); a++ {
			for b := a + 1; b < len(members); b++ {
				i, j := members[a], members[b]
				if i > j {
					i, j = j, i
				}
				if sp, ok := pairScores[[2]int{i, j}]; ok {
					if sp.score > maxScore {
						maxScore = sp.score
					}
					for _, r := range sp.reasons {
						if !reasonSet[r] {
							reasonSet[r] = true
							allReasons = append(allReasons, r)
						}
					}
				}
			}
		}

		// Select master: most populated fields, then earliest created date
		var groupAccounts []SFAccount
		for _, idx := range members {
			groupAccounts = append(groupAccounts, accounts[idx])
		}
		sort.Slice(groupAccounts, func(a, b int) bool {
			pa := countPopulated(groupAccounts[a])
			pb := countPopulated(groupAccounts[b])
			if pa != pb {
				return pa > pb
			}
			return groupAccounts[a].CreatedDate < groupAccounts[b].CreatedDate
		})

		result = append(result, DupeGroup{
			MasterID:     groupAccounts[0].ID,
			Accounts:     groupAccounts,
			Score:        maxScore,
			MatchReasons: allReasons,
		})
	}

	// Sort groups by score descending
	sort.Slice(result, func(i, j int) bool {
		return result[i].Score > result[j].Score
	})

	return result
}

// ---------------------------------------------------------------------------
// State machine helpers
// ---------------------------------------------------------------------------

func isBusy(s AppStatus) bool {
	return s == StatusScanning || s == StatusApplying || s == StatusUndoing ||
		s == StatusScanningDups || s == StatusApplyingDups
}

func startOp(w http.ResponseWriter, status AppStatus, msg string) (context.Context, bool) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if isBusy(state.Status) {
		http.Error(w, "operation already in progress", http.StatusConflict)
		return nil, false
	}
	if !state.ConfigSet {
		http.Error(w, "configure Tiga API key first", http.StatusBadRequest)
		return nil, false
	}
	ctx, cancel := context.WithCancel(context.Background())
	state.cancel = cancel
	state.Status = status
	state.Message = msg
	state.ScanError = ""
	state.Processed = 0
	return ctx, true
}

func cancelled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func jsonReply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

func serveIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}

func handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		TigaKey     string `json:"tiga_key"`
		TigaBaseURL string `json:"tiga_base_url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}

	if req.TigaKey == "" {
		http.Error(w, "tiga_key is required", http.StatusBadRequest)
		return
	}

	baseURL := strings.TrimRight(strings.TrimSpace(req.TigaBaseURL), "/")
	if baseURL == "" {
		baseURL = "https://app.tigalabs.com"
	}

	// Validate by fetching SF token
	tok, err := getSFToken(req.TigaKey, baseURL)
	if err != nil {
		jsonReply(w, map[string]any{"status": "error", "message": "Connection failed: " + err.Error()})
		return
	}

	state.set(func() {
		state.TigaKey = req.TigaKey
		state.TigaBaseURL = baseURL
		state.ConfigSet = true
		state.Message = "Connected to Salesforce."
	})

	jsonReply(w, map[string]any{
		"status":       "ok",
		"instance_url": tok.InstanceURL,
	})
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	state.mu.Lock()
	defer state.mu.Unlock()
	jsonReply(w, state)
}

func handleScanNames(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	resume := r.URL.Query().Get("resume") == "true"

	ctx, ok := startOp(w, StatusScanning, "Fetching Salesforce token...")
	if !ok {
		return
	}

	// Capture config under lock
	state.mu.Lock()
	tigaKey := state.TigaKey
	tigaBaseURL := state.TigaBaseURL
	state.LastOp = "scan-names"
	if !resume {
		state.Changes = nil
		state.Applied = nil
		state.ApplyErrors = nil
		state.TotalRecords = 0
	}
	state.mu.Unlock()

	go func() {
		tok, err := getSFToken(tigaKey, tigaBaseURL)
		if err != nil {
			state.set(func() {
				state.Status = StatusIdle
				state.ScanError = "Failed to get SF token: " + err.Error()
				state.Message = "Error fetching token."
			})
			return
		}

		state.set(func() { state.Message = "Querying Salesforce accounts..." })

		records, err := queryAllAccounts(tok)
		if err != nil {
			state.set(func() {
				state.Status = StatusIdle
				state.ScanError = "Failed to query accounts: " + err.Error()
				state.Message = "Error querying accounts."
			})
			return
		}

		// Cache accounts for dedup
		state.set(func() { state.cachedAccounts = records })

		var skipIDs map[string]bool
		if resume {
			skipIDs = readProcessedIDs("scan")
		} else {
			skipIDs = map[string]bool{}
		}

		var toProcess []SFAccount
		for _, rec := range records {
			if !skipIDs[rec.ID] {
				toProcess = append(toProcess, rec)
			}
		}

		state.set(func() {
			state.TotalRecords = len(records)
			state.Message = fmt.Sprintf("Cleaning %d account names...", len(toProcess))
		})

		type result struct {
			Record SFAccount
			Clean  string
			Err    error
		}

		jobs := make(chan SFAccount, scanWorkers)
		results := make(chan result, scanWorkers)

		var wg sync.WaitGroup
		for i := 0; i < scanWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for rec := range jobs {
					cleaned, err := cleanAccountName(tigaKey, tigaBaseURL, rec.Name)
					results <- result{Record: rec, Clean: cleaned, Err: err}
				}
			}()
		}

		go func() { wg.Wait(); close(results) }()

		go func() {
			defer close(jobs)
			for _, rec := range toProcess {
				if cancelled(ctx) {
					return
				}
				jobs <- rec
			}
		}()

		for res := range results {
			if res.Err != nil {
				logCSV("scan", res.Record.ID, res.Record.Name, "", res.Err.Error())
			} else {
				logCSV("scan", res.Record.ID, res.Record.Name, res.Clean, "")
				if res.Clean != res.Record.Name {
					state.set(func() {
						state.Changes = append(state.Changes, ChangeEntry{
							ID:       res.Record.ID,
							OldName:  res.Record.Name,
							NewName:  res.Clean,
							Selected: true,
						})
					})
				}
			}
			state.set(func() { state.Processed++ })
		}

		if cancelled(ctx) {
			state.set(func() {
				state.Status = StatusStopped
				state.Message = "Scan stopped."
			})
			return
		}

		state.set(func() {
			state.Status = StatusScanned
			state.Message = fmt.Sprintf("Scan complete. Found %d name changes.", len(state.Changes))
		})
	}()

	jsonReply(w, map[string]string{"status": "scanning"})
}

func handleApplyNames(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	state.mu.Lock()
	if state.Status != StatusScanned && state.Status != StatusStopped {
		state.mu.Unlock()
		http.Error(w, "scan first", http.StatusBadRequest)
		return
	}
	var toApply []ChangeEntry
	for _, ch := range state.Changes {
		if ch.Selected {
			toApply = append(toApply, ch)
		}
	}
	tigaKey := state.TigaKey
	tigaBaseURL := state.TigaBaseURL
	state.mu.Unlock()

	if len(toApply) == 0 {
		http.Error(w, "no changes selected", http.StatusBadRequest)
		return
	}

	ctx, ok := startOp(w, StatusApplying, "Fetching Salesforce token...")
	if !ok {
		return
	}

	state.set(func() {
		state.LastOp = "apply-names"
		state.ApplyErrors = nil
		state.TotalRecords = len(toApply)
	})

	go func() {
		tok, err := getSFToken(tigaKey, tigaBaseURL)
		if err != nil {
			state.set(func() {
				state.Status = StatusScanned
				state.ScanError = "Failed to get SF token: " + err.Error()
				state.Message = "Error fetching token."
			})
			return
		}

		state.set(func() { state.Message = "Applying name changes to Salesforce..." })

		for i := 0; i < len(toApply); i += sfBatchSize {
			if cancelled(ctx) {
				state.set(func() {
					state.Status = StatusStopped
					state.Message = "Apply stopped."
				})
				return
			}

			end := i + sfBatchSize
			if end > len(toApply) {
				end = len(toApply)
			}
			batch := toApply[i:end]

			errs := applyBatch(tok, batch)

			for _, ch := range batch {
				if len(errs) == 0 {
					logCSV("apply", ch.ID, ch.OldName, ch.NewName, "")
					state.set(func() {
						state.Applied = append(state.Applied, ch)
						state.Processed++
					})
				} else {
					logCSV("apply", ch.ID, ch.OldName, ch.NewName, strings.Join(errs, "; "))
				}
			}

			if len(errs) > 0 {
				state.set(func() {
					state.ApplyErrors = append(state.ApplyErrors, errs...)
					state.Processed += len(batch)
				})
			}
		}

		state.set(func() {
			state.Status = StatusApplied
			state.Message = fmt.Sprintf("Applied %d name changes.", len(state.Applied))
		})
	}()

	jsonReply(w, map[string]string{"status": "applying"})
}

func handleUndoNames(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	state.mu.Lock()
	if state.Status != StatusApplied {
		state.mu.Unlock()
		http.Error(w, "nothing to undo", http.StatusBadRequest)
		return
	}
	toUndo := make([]ChangeEntry, len(state.Applied))
	copy(toUndo, state.Applied)
	tigaKey := state.TigaKey
	tigaBaseURL := state.TigaBaseURL
	state.mu.Unlock()

	ctx, ok := startOp(w, StatusUndoing, "Fetching Salesforce token...")
	if !ok {
		return
	}

	state.set(func() {
		state.TotalRecords = len(toUndo)
		state.ApplyErrors = nil
	})

	go func() {
		tok, err := getSFToken(tigaKey, tigaBaseURL)
		if err != nil {
			state.set(func() {
				state.Status = StatusApplied
				state.Message = "Error fetching token for undo."
			})
			return
		}

		state.set(func() { state.Message = "Reverting name changes in Salesforce..." })

		var reversed []ChangeEntry
		for _, ch := range toUndo {
			reversed = append(reversed, ChangeEntry{
				ID:      ch.ID,
				OldName: ch.NewName,
				NewName: ch.OldName,
			})
		}

		for i := 0; i < len(reversed); i += sfBatchSize {
			if cancelled(ctx) {
				state.set(func() {
					state.Status = StatusStopped
					state.Message = "Undo stopped."
				})
				return
			}

			end := i + sfBatchSize
			if end > len(reversed) {
				end = len(reversed)
			}
			batch := reversed[i:end]
			errs := applyBatch(tok, batch)

			for _, ch := range batch {
				logCSV("undo", ch.ID, ch.OldName, ch.NewName, strings.Join(errs, "; "))
			}

			state.set(func() {
				if len(errs) > 0 {
					state.ApplyErrors = append(state.ApplyErrors, errs...)
				}
				state.Processed += len(batch)
			})
		}

		state.set(func() {
			state.Status = StatusScanned
			state.Applied = nil
			state.Message = "Undo complete. Records reverted."
		})
	}()

	jsonReply(w, map[string]string{"status": "undoing"})
}

func handleScanDupes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	ctx, ok := startOp(w, StatusScanningDups, "Fetching Salesforce accounts for dedup...")
	if !ok {
		return
	}

	state.mu.Lock()
	tigaKey := state.TigaKey
	tigaBaseURL := state.TigaBaseURL
	cached := state.cachedAccounts
	state.DupeGroups = nil
	state.DupeApplied = nil
	state.mu.Unlock()

	go func() {
		var accounts []SFAccount

		if len(cached) > 0 {
			accounts = cached
			state.set(func() { state.Message = "Using cached accounts..." })
		} else {
			tok, err := getSFToken(tigaKey, tigaBaseURL)
			if err != nil {
				state.set(func() {
					state.Status = StatusIdle
					state.ScanError = "Failed to get SF token: " + err.Error()
					state.Message = "Error fetching token."
				})
				return
			}

			state.set(func() { state.Message = "Querying Salesforce accounts..." })

			var err2 error
			accounts, err2 = queryAllAccounts(tok)
			if err2 != nil {
				state.set(func() {
					state.Status = StatusIdle
					state.ScanError = "Failed to query accounts: " + err2.Error()
					state.Message = "Error querying accounts."
				})
				return
			}
			state.set(func() { state.cachedAccounts = accounts })
		}

		if cancelled(ctx) {
			state.set(func() {
				state.Status = StatusStopped
				state.Message = "Dedup scan stopped."
			})
			return
		}

		state.set(func() {
			state.TotalRecords = len(accounts)
			state.Message = fmt.Sprintf("Analyzing %d accounts for duplicates...", len(accounts))
		})

		groups := findDuplicateGroups(accounts)

		totalDupes := 0
		for _, g := range groups {
			totalDupes += len(g.Accounts) - 1
		}

		state.set(func() {
			state.DupeGroups = groups
			state.Processed = len(accounts)
			state.Status = StatusScannedDups
			state.Message = fmt.Sprintf("Found %d duplicate groups (%d records to mark).", len(groups), totalDupes)
		})
	}()

	jsonReply(w, map[string]string{"status": "scanning_dupes"})
}

func handleApplyDupes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	state.mu.Lock()
	if state.Status != StatusScannedDups {
		state.mu.Unlock()
		http.Error(w, "scan for dupes first", http.StatusBadRequest)
		return
	}
	groups := make([]DupeGroup, len(state.DupeGroups))
	copy(groups, state.DupeGroups)
	tigaKey := state.TigaKey
	tigaBaseURL := state.TigaBaseURL
	state.mu.Unlock()

	// Build change entries: mark non-master accounts with [DUPLICATE] prefix
	var toApply []ChangeEntry
	for _, g := range groups {
		for _, acc := range g.Accounts {
			if acc.ID != g.MasterID && !strings.HasPrefix(acc.Name, "[DUPLICATE] ") {
				toApply = append(toApply, ChangeEntry{
					ID:       acc.ID,
					OldName:  acc.Name,
					NewName:  "[DUPLICATE] " + acc.Name,
					Selected: true,
				})
			}
		}
	}

	if len(toApply) == 0 {
		http.Error(w, "no duplicates to mark", http.StatusBadRequest)
		return
	}

	ctx, ok := startOp(w, StatusApplyingDups, "Marking duplicates in Salesforce...")
	if !ok {
		return
	}

	state.set(func() {
		state.TotalRecords = len(toApply)
		state.ApplyErrors = nil
	})

	go func() {
		tok, err := getSFToken(tigaKey, tigaBaseURL)
		if err != nil {
			state.set(func() {
				state.Status = StatusScannedDups
				state.ScanError = "Failed to get SF token: " + err.Error()
				state.Message = "Error fetching token."
			})
			return
		}

		for i := 0; i < len(toApply); i += sfBatchSize {
			if cancelled(ctx) {
				state.set(func() {
					state.Status = StatusStopped
					state.Message = "Dedup apply stopped."
				})
				return
			}

			end := i + sfBatchSize
			if end > len(toApply) {
				end = len(toApply)
			}
			batch := toApply[i:end]

			errs := applyBatch(tok, batch)

			for _, ch := range batch {
				if len(errs) == 0 {
					logCSV("dedup", ch.ID, ch.OldName, ch.NewName, "")
				} else {
					logCSV("dedup", ch.ID, ch.OldName, ch.NewName, strings.Join(errs, "; "))
				}
			}

			state.set(func() {
				if len(errs) > 0 {
					state.ApplyErrors = append(state.ApplyErrors, errs...)
				}
				state.Processed += len(batch)
			})
		}

		state.set(func() {
			state.DupeApplied = groups
			state.Status = StatusAppliedDups
			state.Message = fmt.Sprintf("Marked %d duplicate accounts.", len(toApply))
		})
	}()

	jsonReply(w, map[string]string{"status": "applying_dupes"})
}

func handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	state.mu.Lock()
	if state.cancel != nil {
		state.cancel()
	}
	state.mu.Unlock()
	jsonReply(w, map[string]string{"status": "stopping"})
}

func handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	state.mu.Lock()
	if state.cancel != nil {
		state.cancel()
	}
	state.mu.Unlock()

	time.Sleep(200 * time.Millisecond)

	state.set(func() {
		configSet := state.ConfigSet
		tigaKey := state.TigaKey
		tigaBaseURL := state.TigaBaseURL

		state.Status = StatusIdle
		state.Message = "Ready to scan."
		state.Changes = nil
		state.Applied = nil
		state.ApplyErrors = nil
		state.ScanError = ""
		state.TotalRecords = 0
		state.Processed = 0
		state.cancel = nil
		state.DupeGroups = nil
		state.DupeApplied = nil
		state.cachedAccounts = nil

		state.ConfigSet = configSet
		state.TigaKey = tigaKey
		state.TigaBaseURL = tigaBaseURL
	})
	jsonReply(w, map[string]string{"status": "idle"})
}

func handleToggle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ID       string `json:"id"`
		Selected bool   `json:"selected"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	state.set(func() {
		for i := range state.Changes {
			if state.Changes[i].ID == req.ID {
				state.Changes[i].Selected = req.Selected
				break
			}
		}
	})
	jsonReply(w, map[string]string{"status": "ok"})
}

func handleSetMaster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		GroupIndex int    `json:"group_index"`
		MasterID   string `json:"master_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	state.set(func() {
		if req.GroupIndex >= 0 && req.GroupIndex < len(state.DupeGroups) {
			state.DupeGroups[req.GroupIndex].MasterID = req.MasterID
		}
	})
	jsonReply(w, map[string]string{"status": "ok"})
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	initCSVLog()

	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/api/config", handleConfig)
	http.HandleFunc("/api/status", handleStatus)
	http.HandleFunc("/api/scan-names", handleScanNames)
	http.HandleFunc("/api/apply-names", handleApplyNames)
	http.HandleFunc("/api/undo-names", handleUndoNames)
	http.HandleFunc("/api/scan-dupes", handleScanDupes)
	http.HandleFunc("/api/apply-dupes", handleApplyDupes)
	http.HandleFunc("/api/stop", handleStop)
	http.HandleFunc("/api/reset", handleReset)
	http.HandleFunc("/api/toggle", handleToggle)
	http.HandleFunc("/api/set-master", handleSetMaster)

	log.Println("Listening on http://localhost:8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}
