package repository

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"golang.org/x/sys/unix"
)

// A slot is physically allocated before dispatch. Reusing that inode reduces
// late capacity risk; CoW/snapshots or device failures can still require new
// allocation or fail I/O. The healthy PG path remains available in that case.
// Unknown crash remnants count toward the limit and are never aged out.
const chatReceiptSlotBytes = 32 * 1024
const chatReceiptSlotLimit = 1024

type chatReceiptJournal struct {
	dir   string
	limit int
}
type chatReceiptJournalFrame struct {
	Version  int             `json:"version"`
	SHA256   string          `json:"sha256"`
	Envelope json.RawMessage `json:"envelope"`
}

func newChatReceiptJournal(dir string) (*chatReceiptJournal, error) {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return nil, errors.New("receipt journal requires an absolute clean directory")
	}
	// Reject symlinks in every configured component, including the existing
	// persistent parent. Deployment separately verifies that parent is a mount.
	for path := filepath.Dir(dir); ; path = filepath.Dir(path) {
		st, err := os.Lstat(path)
		if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("invalid receipt journal parent")
		}
		if path == "/" {
			break
		}
	}
	if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	parent, err := os.Open(filepath.Dir(dir))
	if err != nil {
		return nil, err
	}
	syncErr := parent.Sync()
	closeErr := parent.Close()
	if err = errors.Join(syncErr, closeErr); err != nil {
		return nil, err
	}
	st, err := os.Lstat(dir)
	if err != nil || !st.IsDir() || st.Mode().Perm() != 0700 {
		return nil, errors.New("receipt journal directory must be private")
	}
	if s, ok := st.Sys().(*syscall.Stat_t); ok && int(s.Uid) != os.Geteuid() {
		return nil, errors.New("receipt journal directory owner mismatch")
	}
	j := &chatReceiptJournal{dir: dir, limit: chatReceiptSlotLimit}
	err = j.locked(func(root *os.Root, directory *os.File) error {
		// Probe the already-required lock inode; a crash leaves no separate
		// O_EXCL probe name that could permanently block the next startup.
		f, err := root.OpenFile(".lock", os.O_RDWR|unix.O_NOFOLLOW, 0600)
		if err != nil {
			return err
		}
		defer f.Close()
		prior, err := io.ReadAll(io.LimitReader(f, 6))
		if err != nil || !bytes.HasPrefix([]byte("ready"), prior) {
			return errors.New("invalid receipt journal lock content")
		}
		n, writeErr := f.WriteAt([]byte("ready"), 0)
		if n != 5 && writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		syncErr := f.Sync()
		if err := directory.Sync(); err != nil {
			return err
		}
		return errors.Join(writeErr, syncErr)
	})
	if err != nil {
		return nil, err
	}
	return j, nil
}

func (j *chatReceiptJournal) locked(fn func(*os.Root, *os.File) error) error {
	st, err := os.Lstat(j.dir)
	if err != nil || !st.IsDir() || st.Mode().Perm() != 0700 {
		return errors.New("receipt journal directory changed")
	}
	owner, ok := st.Sys().(*syscall.Stat_t)
	if !ok || int(owner.Uid) != os.Geteuid() {
		return errors.New("receipt journal directory owner changed")
	}
	root, err := os.OpenRoot(j.dir)
	if err != nil {
		return err
	}
	defer root.Close()
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	defer directory.Close()
	opened, err := directory.Stat()
	if err != nil || !os.SameFile(st, opened) {
		return errors.New("receipt journal directory identity changed")
	}
	f, err := root.OpenFile(".lock", os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err = f.Stat()
	if err != nil || !privateJournalFile(st, -1) {
		return errors.New("invalid receipt journal lock")
	}
	// Leave most of the existing five-second completion budget for the
	// authoritative PG path even when another process holds the file lock.
	deadline := time.Now().Add(time.Second)
	for {
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if (!errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN)) || !time.Now().Before(deadline) {
			return errors.New("receipt journal lock unavailable")
		}
		time.Sleep(5 * time.Millisecond)
	}
	defer unix.Flock(int(f.Fd()), unix.LOCK_UN)
	return fn(root, directory)
}

func privateJournalFile(st os.FileInfo, size int64) bool {
	if st == nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || (size >= 0 && st.Size() != size) {
		return false
	}
	s, ok := st.Sys().(*syscall.Stat_t)
	return ok && int(s.Uid) == os.Geteuid() && s.Nlink == 1
}

func receiptJournalName(key int64, id string) string {
	sum := sha256.Sum256([]byte(strconv.FormatInt(key, 10) + ":" + id))
	return hex.EncodeToString(sum[:]) + ".slot"
}

func validJournalClaim(c *service.ChatUsageReceiptClaim) bool {
	if c == nil || c.APIKeyID <= 0 || !service.ValidUsageReceiptID(c.RequestID) || !service.ValidUsageReceiptID(c.Token) || c.Endpoint != service.ChatReceiptEndpoint || c.Model == "" || len(c.Model) > 256 || strings.ContainsAny(c.Model, "\r\n\x00") || strings.Contains(c.Model, "://") || len(c.PayloadHash) != 64 {
		return false
	}
	_, err := hex.DecodeString(c.PayloadHash)
	return err == nil
}

func validJournalEnvelope(e chatReceiptEnvelope) bool {
	if !validJournalClaim(e.Claim) {
		return false
	}
	if e.State == "pending" {
		return e.Receipt == nil && e.AccountID == 0 && e.BillingRequestID == ""
	}
	return e.State == "final" && e.AccountID > 0 && e.BillingRequestID != "" && len(e.BillingRequestID) <= 256 && !strings.ContainsAny(e.BillingRequestID, "\r\n\x00") && !strings.Contains(e.BillingRequestID, "://") && service.ValidateChatUsageReceipt(e.Receipt) && e.Receipt.RequestID == e.Claim.RequestID && e.Receipt.Model == e.Claim.Model && e.Receipt.InboundEndpoint == e.Claim.Endpoint
}

func encodeJournalEnvelope(e chatReceiptEnvelope) ([]byte, error) {
	if !validJournalEnvelope(e) {
		return nil, service.ErrChatReceiptConflict
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(payload)
	frame, err := json.Marshal(chatReceiptJournalFrame{Version: 1, SHA256: hex.EncodeToString(sum[:]), Envelope: payload})
	if err != nil {
		return nil, err
	}
	if len(frame) > chatReceiptSlotBytes {
		return nil, errors.New("receipt journal record exceeds reserved slot")
	}
	return append(frame, bytes.Repeat([]byte{' '}, chatReceiptSlotBytes-len(frame))...), nil
}

func readJournalSlot(root *os.Root, name string) (chatReceiptEnvelope, error) {
	var envelope chatReceiptEnvelope
	f, err := root.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return envelope, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !privateJournalFile(st, chatReceiptSlotBytes) {
		return envelope, errors.New("invalid receipt journal slot metadata")
	}
	data, err := io.ReadAll(io.LimitReader(f, chatReceiptSlotBytes+1))
	if err != nil || len(data) != chatReceiptSlotBytes {
		return envelope, errors.New("invalid receipt journal slot length")
	}
	var frame chatReceiptJournalFrame
	if json.Unmarshal(bytes.TrimRight(data, " "), &frame) != nil || frame.Version != 1 {
		return envelope, errors.New("invalid receipt journal frame")
	}
	sum := sha256.Sum256(frame.Envelope)
	if frame.SHA256 != hex.EncodeToString(sum[:]) || json.Unmarshal(frame.Envelope, &envelope) != nil {
		return envelope, errors.New("receipt journal checksum mismatch")
	}
	canonical, err := encodeJournalEnvelope(envelope)
	if err != nil || !bytes.Equal(canonical, data) {
		return envelope, errors.New("noncanonical receipt journal record")
	}
	return envelope, nil
}

func (j *chatReceiptJournal) reserve(claim *service.ChatUsageReceiptClaim) error {
	envelope := chatReceiptEnvelope{State: "pending", Claim: claim}
	data, err := encodeJournalEnvelope(envelope)
	if err != nil {
		return err
	}
	return j.locked(func(root *os.Root, directory *os.File) error {
		name := receiptJournalName(claim.APIKeyID, claim.RequestID)
		if existing, err := readJournalSlot(root, name); err == nil {
			if existing.State == "pending" && *existing.Claim == *claim {
				return nil
			}
			return service.ErrChatReceiptConflict
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		entries, err := directory.ReadDir(-1)
		if err != nil {
			return err
		}
		count := 0
		for _, entry := range entries {
			if entry.Name() == ".lock" {
				continue
			}
			st, err := entry.Info()
			if err != nil || !st.Mode().IsRegular() || st.Size() != chatReceiptSlotBytes {
				return errors.New("invalid receipt journal inventory")
			}
			count++
		}
		if count >= j.limit {
			return errors.New("receipt journal capacity exhausted")
		}
		f, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY|unix.O_NOFOLLOW, 0600)
		if err != nil {
			return err
		}
		n, writeErr := f.Write(data)
		if n != len(data) && writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		syncErr := f.Sync()
		var allocated unix.Stat_t
		blockErr := unix.Fstat(int(f.Fd()), &allocated)
		if blockErr == nil && allocated.Blocks*512 < chatReceiptSlotBytes {
			blockErr = errors.New("receipt reservation is sparse")
		}
		closeErr := f.Close()
		if err := errors.Join(writeErr, syncErr, blockErr, closeErr, directory.Sync()); err != nil {
			return err
		}
		// Writing all bytes plus fsync materializes blocks; truncate alone would
		// be a sparse reservation and would not protect the late final write.
		return nil
	})
}

func (j *chatReceiptJournal) finalize(envelope chatReceiptEnvelope) error {
	data, err := encodeJournalEnvelope(envelope)
	if err != nil {
		return err
	}
	return j.locked(func(root *os.Root, directory *os.File) error {
		name := receiptJournalName(envelope.Claim.APIKeyID, envelope.Claim.RequestID)
		existing, err := readJournalSlot(root, name)
		if err != nil {
			return err
		}
		if *existing.Claim != *envelope.Claim {
			return service.ErrChatReceiptConflict
		}
		if existing.State == "final" {
			prior, _ := encodeJournalEnvelope(existing)
			if bytes.Equal(prior, data) {
				return nil
			}
			return service.ErrChatReceiptConflict
		}
		f, err := root.OpenFile(name, os.O_WRONLY|unix.O_NOFOLLOW, 0)
		if err != nil {
			return err
		}
		n, writeErr := f.WriteAt(data, 0)
		if n != len(data) && writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		syncErr := f.Sync()
		closeErr := f.Close()
		// The same slot name is intentionally retained: a complete final is
		// discoverable immediately after fsync, without a rename crash gap.
		return errors.Join(writeErr, syncErr, closeErr, directory.Sync())
	})
}

func (j *chatReceiptJournal) load(key int64, id string) (*chatReceiptEnvelope, error) {
	var result *chatReceiptEnvelope
	err := j.locked(func(root *os.Root, _ *os.File) error {
		e, err := readJournalSlot(root, receiptJournalName(key, id))
		if err != nil {
			return err
		}
		if e.Claim.APIKeyID != key || e.Claim.RequestID != id {
			return service.ErrChatReceiptConflict
		}
		if e.State == "final" {
			result = &e
		}
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return result, err
}

func (j *chatReceiptJournal) remove(expected chatReceiptEnvelope) error {
	return j.locked(func(root *os.Root, directory *os.File) error {
		name := receiptJournalName(expected.Claim.APIKeyID, expected.Claim.RequestID)
		existing, err := readJournalSlot(root, name)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if *existing.Claim != *expected.Claim {
			return service.ErrChatReceiptConflict
		}
		if expected.State == "pending" && existing.State == "final" {
			return nil
		}
		left, _ := encodeJournalEnvelope(existing)
		right, err := encodeJournalEnvelope(expected)
		if err != nil || !bytes.Equal(left, right) {
			return service.ErrChatReceiptConflict
		}
		if err := root.Remove(name); err != nil {
			return err
		}
		return directory.Sync()
	})
}
