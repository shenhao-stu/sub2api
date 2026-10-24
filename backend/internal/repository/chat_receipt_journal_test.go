package repository

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func journalFixture(t *testing.T) (*chatReceiptJournal, chatReceiptEnvelope) {
	t.Helper()
	parent, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	j, err := newChatReceiptJournal(filepath.Join(parent, "receipts"))
	require.NoError(t, err)
	c := &service.ChatUsageReceiptClaim{APIKeyID: 11, RequestID: uuid.NewString(), Token: uuid.NewString(), Model: "gemini-synthetic", Endpoint: service.ChatReceiptEndpoint, PayloadHash: strings.Repeat("a", 64)}
	r := &service.UsageReceipt{Version: 2, Object: "sub2api.usage_receipt", Source: "wire_final", RequestID: c.RequestID, Model: c.Model, InboundEndpoint: c.Endpoint, InputTokens: 100, OutputTokens: 7, BillingMode: "token", TerminalState: "completed", HTTPStatus: 200, RecordedAt: time.Now().UTC().Truncate(time.Microsecond)}
	return j, chatReceiptEnvelope{State: "final", Claim: c, AccountID: 3, BillingRequestID: "client:" + uuid.NewString(), Receipt: r}
}

func TestChatReceiptJournalRestartRecoversFailedAndUnknownCommit(t *testing.T) {
	for _, mode := range []string{"write_failed", "commit_ack_lost", "readback_failed"} {
		t.Run(mode, func(t *testing.T) {
			j, e := journalFixture(t)
			db, m, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			r := newUsageLogRepositoryWithSQL(nil, db)
			r.chatJournal = j
			require.NoError(t, r.PrepareChatUsageReceipt(context.Background(), e.Claim))
			m.ExpectExec("INSERT INTO provider_usage_receipts").WillReturnResult(sqlmock.NewResult(0, 1))
			won, err := r.ClaimChatUsageReceipt(context.Background(), e.Claim)
			require.NoError(t, err)
			require.True(t, won)
			if mode == "readback_failed" {
				m.ExpectExec("UPDATE provider_usage_receipts").WillReturnResult(sqlmock.NewResult(0, 1))
				m.ExpectQuery("SELECT payload=").WillReturnError(errors.New("synthetic readback outage"))
			} else {
				m.ExpectExec("UPDATE provider_usage_receipts").WillReturnError(errors.New("synthetic transaction result unavailable"))
			}
			err = r.CompleteChatUsageReceipt(context.Background(), e.Claim, e.AccountID, e.BillingRequestID, e.Receipt)
			require.Error(t, err)
			frozen, err := j.load(e.Claim.APIKeyID, e.Claim.RequestID)
			require.NoError(t, err)
			require.Equal(t, e, *frozen)
			// Process restart: no in-memory receipt survives.
			j2, err := newChatReceiptJournal(j.dir)
			require.NoError(t, err)
			restarted := newUsageLogRepositoryWithSQL(nil, db)
			restarted.chatJournal = j2
			stored := e
			if mode == "write_failed" {
				stored = chatReceiptEnvelope{State: "pending", Claim: e.Claim}
			}
			payload, _ := json.Marshal(stored)
			m.ExpectQuery("SELECT payload FROM provider_usage_receipts").WillReturnRows(sqlmock.NewRows([]string{"payload"}).AddRow(payload))
			if mode == "write_failed" {
				m.ExpectExec("UPDATE provider_usage_receipts").WillReturnResult(sqlmock.NewResult(0, 1))
			}
			m.ExpectQuery("SELECT payload=").WillReturnRows(sqlmock.NewRows([]string{"equal"}).AddRow(true))
			got, err := restarted.GetUsageReceipt(context.Background(), e.Claim.APIKeyID, e.Claim.RequestID)
			require.NoError(t, err)
			require.Equal(t, e.Receipt, got)
			_, err = os.Stat(filepath.Join(j.dir, receiptJournalName(e.Claim.APIKeyID, e.Claim.RequestID)))
			require.ErrorIs(t, err, os.ErrNotExist)
			require.NoError(t, m.ExpectationsWereMet())
		})
	}
}

func TestChatReceiptJournalConflictNeverResurrectsOrRebinds(t *testing.T) {
	for _, mode := range []string{"deleted", "different_claim", "different_account", "different_meter"} {
		t.Run(mode, func(t *testing.T) {
			j, e := journalFixture(t)
			require.NoError(t, j.reserve(e.Claim))
			require.NoError(t, j.finalize(e))
			db, m, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			r := newUsageLogRepositoryWithSQL(nil, db)
			r.chatJournal = j
			initialRows := sqlmock.NewRows([]string{"payload"})
			if mode != "deleted" {
				stored := e
				if mode == "different_claim" {
					c := *e.Claim
					c.Token = uuid.NewString()
					stored = chatReceiptEnvelope{State: "pending", Claim: &c}
				} else if mode == "different_account" {
					stored.AccountID++
				} else {
					r := *e.Receipt
					r.OutputTokens++
					stored.Receipt = &r
				}
				p, _ := json.Marshal(stored)
				initialRows.AddRow(p)
			}
			m.ExpectQuery("SELECT payload FROM provider_usage_receipts").WillReturnRows(initialRows)
			if mode == "deleted" || mode == "different_claim" {
				m.ExpectExec("UPDATE provider_usage_receipts").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			rows := sqlmock.NewRows([]string{"equal"})
			if mode != "deleted" {
				rows.AddRow(false)
			}
			m.ExpectQuery("SELECT payload=").WillReturnRows(rows)
			got, err := r.GetUsageReceipt(context.Background(), e.Claim.APIKeyID, e.Claim.RequestID)
			require.ErrorIs(t, err, service.ErrChatReceiptConflict)
			require.Nil(t, got)
			frozen, err := j.load(e.Claim.APIKeyID, e.Claim.RequestID)
			require.NoError(t, err)
			require.Equal(t, e, *frozen)
			require.NoError(t, m.ExpectationsWereMet())
		})
	}
}

func TestChatReceiptJournalUnknownAndFinalRelease(t *testing.T) {
	j, e := journalFixture(t)
	require.NoError(t, j.reserve(e.Claim))
	got, err := j.load(e.Claim.APIKeyID, e.Claim.RequestID)
	require.NoError(t, err)
	require.Nil(t, got, "reservation is not zero usage")
	j2, err := newChatReceiptJournal(j.dir)
	require.NoError(t, err)
	got, err = j2.load(e.Claim.APIKeyID, e.Claim.RequestID)
	require.NoError(t, err)
	require.Nil(t, got)
	st, err := os.Stat(filepath.Join(j.dir, receiptJournalName(e.Claim.APIKeyID, e.Claim.RequestID)))
	require.NoError(t, err)
	require.Equal(t, int64(chatReceiptSlotBytes), st.Size())
	require.NoError(t, j.finalize(e))
	// Forward's deferred no-meter cleanup cannot erase a completed final, even
	// when the process ended before a separate rename/PG update would occur.
	require.NoError(t, j.remove(chatReceiptEnvelope{State: "pending", Claim: e.Claim}))
	got, err = j2.load(e.Claim.APIKeyID, e.Claim.RequestID)
	require.NoError(t, err)
	require.Equal(t, e, *got)
	changed := e
	changed.AccountID++
	require.ErrorIs(t, j.finalize(changed), service.ErrChatReceiptConflict)
	wrong := *e.Claim
	wrong.Token = uuid.NewString()
	require.ErrorIs(t, j.remove(chatReceiptEnvelope{State: "pending", Claim: &wrong}), service.ErrChatReceiptConflict)
	other, err := j.load(12, e.Claim.RequestID)
	require.NoError(t, err)
	require.Nil(t, other)
}

func TestChatReceiptJournalCrashHelper(t *testing.T) {
	if os.Getenv("SUB_CHAT_JOURNAL_CRASH_HELPER") != "1" {
		return
	}
	j, err := newChatReceiptJournal(os.Getenv("SUB_CHAT_JOURNAL_CRASH_DIR"))
	if err != nil {
		os.Exit(80)
	}
	var e chatReceiptEnvelope
	if json.Unmarshal([]byte(os.Getenv("SUB_CHAT_JOURNAL_CRASH_ENVELOPE")), &e) != nil || j.finalize(e) != nil {
		os.Exit(81)
	}
	// Intentional process exit after file+directory fsync and before any PG
	// request or caller cleanup. No in-memory recovery state is retained.
	os.Exit(23)
}

func TestChatReceiptJournalFsyncedFinalSurvivesProcessExit(t *testing.T) {
	j, e := journalFixture(t)
	require.NoError(t, j.reserve(e.Claim))
	payload, err := json.Marshal(e)
	require.NoError(t, err)
	cmd := exec.Command(os.Args[0], "-test.run=^TestChatReceiptJournalCrashHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "SUB_CHAT_JOURNAL_CRASH_HELPER=1", "SUB_CHAT_JOURNAL_CRASH_DIR="+j.dir, "SUB_CHAT_JOURNAL_CRASH_ENVELOPE="+string(payload))
	err = cmd.Run()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 23, exit.ExitCode())
	restarted, err := newChatReceiptJournal(j.dir)
	require.NoError(t, err)
	got, err := restarted.load(e.Claim.APIKeyID, e.Claim.RequestID)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, e, *got)
}

func TestChatReceiptJournalConcurrentCapacity(t *testing.T) {
	j, e := journalFixture(t)
	j.limit = 4
	var wins atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := *e.Claim
			c.RequestID = uuid.NewString()
			c.Token = uuid.NewString()
			if j.reserve(&c) == nil {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int32(4), wins.Load())
	entries, err := os.ReadDir(j.dir)
	require.NoError(t, err)
	require.Len(t, entries, 5)
}

func TestChatReceiptJournalRejectsUnsafeFiles(t *testing.T) {
	for _, mode := range []string{"symlink", "hardlink", "mode", "truncated", "oversize", "bad_checksum", "duplicate", "directory"} {
		t.Run(mode, func(t *testing.T) {
			j, e := journalFixture(t)
			require.NoError(t, j.reserve(e.Claim))
			path := filepath.Join(j.dir, receiptJournalName(e.Claim.APIKeyID, e.Claim.RequestID))
			switch mode {
			case "symlink":
				require.NoError(t, os.Remove(path))
				require.NoError(t, os.Symlink(filepath.Join(j.dir, ".lock"), path))
			case "hardlink":
				require.NoError(t, os.Link(path, filepath.Join(j.dir, "another")))
			case "mode":
				require.NoError(t, os.Chmod(path, 0644))
			case "truncated":
				require.NoError(t, os.Truncate(path, 3))
			case "oversize":
				require.NoError(t, os.Truncate(path, chatReceiptSlotBytes+1))
			case "bad_checksum":
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				data[40] ^= 1
				require.NoError(t, os.WriteFile(path, data, 0600))
			case "duplicate":
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				copy(data, []byte(`{"version":1,"version":1,`))
				require.NoError(t, os.WriteFile(path, data, 0600))
			case "directory":
				require.NoError(t, os.Remove(path))
				require.NoError(t, os.Mkdir(path, 0700))
			}
			got, err := j.load(e.Claim.APIKeyID, e.Claim.RequestID)
			require.Error(t, err)
			require.Nil(t, got)
			require.Error(t, j.remove(chatReceiptEnvelope{State: "pending", Claim: e.Claim}))
			_, err = os.Lstat(path)
			require.NoError(t, err, "invalid evidence must not be silently deleted")
		})
	}
}

func TestChatReceiptJournalStartupCrashLeavesNoExclusiveProbe(t *testing.T) {
	for _, prefix := range []string{"", "r", "rea", "ready"} {
		t.Run("lock_prefix_"+prefix, func(t *testing.T) {
			j, _ := journalFixture(t)
			require.NoError(t, os.WriteFile(filepath.Join(j.dir, ".lock"), []byte(prefix), 0600))
			_, err := newChatReceiptJournal(j.dir)
			require.NoError(t, err)
			data, err := os.ReadFile(filepath.Join(j.dir, ".lock"))
			require.NoError(t, err)
			require.Equal(t, "ready", string(data))
			_, err = os.Lstat(filepath.Join(j.dir, ".probe"))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
	j, _ := journalFixture(t)
	require.NoError(t, os.WriteFile(filepath.Join(j.dir, ".lock"), []byte("unrelated data"), 0600))
	_, err := newChatReceiptJournal(j.dir)
	require.Error(t, err)
	data, err := os.ReadFile(filepath.Join(j.dir, ".lock"))
	require.NoError(t, err)
	require.Equal(t, "unrelated data", string(data))
}

func TestChatReceiptJournalIOFailurePreservesHealthyPGWriteAndRead(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			j, e := journalFixture(t)
			require.NoError(t, j.reserve(e.Claim))
			require.NoError(t, os.Chmod(j.dir, 0750))
			t.Cleanup(func() { _ = os.Chmod(j.dir, 0700) })
			db, m, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			r := newUsageLogRepositoryWithSQL(nil, db)
			r.chatJournal = j
			m.ExpectExec("UPDATE provider_usage_receipts").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectQuery("SELECT payload=").WillReturnRows(sqlmock.NewRows([]string{"equal"}).AddRow(true))
			require.NoError(t, r.CompleteChatUsageReceipt(context.Background(), e.Claim, e.AccountID, e.BillingRequestID, e.Receipt))
			var payload []byte
			if version == 1 {
				v := *e.Receipt
				v.Version = 1
				payload, _ = json.Marshal(v)
			} else {
				payload, _ = json.Marshal(e)
			}
			m.ExpectQuery("SELECT payload FROM provider_usage_receipts").WillReturnRows(sqlmock.NewRows([]string{"payload"}).AddRow(payload))
			got, err := r.GetUsageReceipt(context.Background(), e.Claim.APIKeyID, e.Claim.RequestID)
			require.NoError(t, err)
			require.Equal(t, 7, got.OutputTokens)
			require.Equal(t, version, got.Version)
			require.NoError(t, m.ExpectationsWereMet())
		})
	}
}

func TestChatReceiptJournalLockCannotConsumePGFallbackBudget(t *testing.T) {
	j, e := journalFixture(t)
	require.NoError(t, j.reserve(e.Claim))
	holdJournalLockInProcess(t, j.dir)
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	r := newUsageLogRepositoryWithSQL(nil, db)
	r.chatJournal = j
	m.ExpectExec("UPDATE provider_usage_receipts").WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectQuery("SELECT payload=").WillReturnRows(sqlmock.NewRows([]string{"equal"}).AddRow(true))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	require.NoError(t, r.CompleteChatUsageReceipt(ctx, e.Claim, e.AccountID, e.BillingRequestID, e.Receipt))
	require.NoError(t, ctx.Err())
	require.Less(t, time.Since(start), 3*time.Second)
	require.NoError(t, m.ExpectationsWereMet())
}

func TestChatReceiptJournalLockHelper(t *testing.T) {
	if os.Getenv("SUB_CHAT_JOURNAL_LOCK_HELPER") != "1" {
		return
	}
	f, err := os.OpenFile(filepath.Join(os.Getenv("SUB_CHAT_JOURNAL_LOCK_DIR"), ".lock"), os.O_RDWR, 0)
	if err != nil {
		os.Exit(82)
	}
	if unix.Flock(int(f.Fd()), unix.LOCK_EX) != nil {
		os.Exit(83)
	}
	_, _ = os.Stdout.WriteString("locked\n")
	done := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, os.Stdin); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		os.Exit(84)
	}
	_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
	_ = f.Close()
}

func holdJournalLockInProcess(t *testing.T, dir string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestChatReceiptJournalLockHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "SUB_CHAT_JOURNAL_LOCK_HELPER=1", "SUB_CHAT_JOURNAL_LOCK_DIR="+dir)
	in, err := cmd.StdinPipe()
	require.NoError(t, err)
	out, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = in.Close()
		if err := cmd.Wait(); err != nil {
			t.Errorf("lock helper did not exit cleanly: %v", err)
		}
	})
	line, err := bufio.NewReader(out).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "locked\n", line)
}

func TestChatReceiptJournalConcurrentConsumerRereadsPGAfterSlotRemoval(t *testing.T) {
	j, e := journalFixture(t)
	db, m, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	r := newUsageLogRepositoryWithSQL(nil, db)
	r.chatJournal = j
	pending, _ := json.Marshal(chatReceiptEnvelope{State: "pending", Claim: e.Claim})
	final, _ := json.Marshal(e)
	m.ExpectQuery("SELECT payload FROM provider_usage_receipts").WillReturnRows(sqlmock.NewRows([]string{"payload"}).AddRow(pending))
	m.ExpectQuery("SELECT payload FROM provider_usage_receipts").WillReturnRows(sqlmock.NewRows([]string{"payload"}).AddRow(final))
	got, err := r.GetUsageReceipt(context.Background(), e.Claim.APIKeyID, e.Claim.RequestID)
	require.NoError(t, err)
	require.Equal(t, e.Receipt, got)
	require.NoError(t, m.ExpectationsWereMet())
}
