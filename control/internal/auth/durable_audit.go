package auth

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	auditJournalFormatVersion = 1
	maxAuditRecordBytes       = 16 << 10
	maxAuditPageSize          = 1_000
)

var zeroAuditDigest = [sha256.Size]byte{}

// AuditEventDocument is the stable, credential-free export representation.
type AuditEventDocument struct {
	EventTimeUnixMS      string               `json:"event_time_unix_ms"`
	RequestID            string               `json:"request_id"`
	PrincipalID          string               `json:"principal_id"`
	PolicyID             string               `json:"policy_id"`
	AuthenticationMethod AuthenticationMethod `json:"authentication_method,omitempty"`
	Action               Action               `json:"action"`
	Decision             Decision             `json:"decision"`
	Reason               DecisionReason       `json:"reason"`
	Scope                Scope                `json:"scope"`
}

// AuditJournalRecord carries one independently verifiable hash-chain link.
// Sequence is decimal text so browser clients never round a uint64 value.
type AuditJournalRecord struct {
	FormatVersion  int                `json:"format_version"`
	Sequence       string             `json:"sequence"`
	PreviousSHA256 string             `json:"previous_sha256"`
	Event          AuditEventDocument `json:"event"`
	RecordSHA256   string             `json:"record_sha256"`
}

// AuditPage is one bounded immutable export page.
type AuditPage struct {
	Records      []AuditJournalRecord `json:"records"`
	NextSequence string               `json:"next_sequence"`
	EndOfJournal bool                 `json:"end_of_journal"`
}

// AuditReader exposes integrity-verified records without mutation methods.
type AuditReader interface {
	ReadAuditPage(context.Context, uint64, int, Principal) (AuditPage, error)
}

// DurableAuditJournal appends canonical JSON records and fsyncs every accepted
// decision. Existing bytes are fully verified before the journal is writable.
type DurableAuditJournal struct {
	mutex        sync.Mutex
	file         *os.File
	path         string
	lastSequence uint64
	lastDigest   [sha256.Size]byte
	failed       error
}

// OpenDurableAuditJournal opens or creates one owner-only append journal.
func OpenDurableAuditJournal(path string) (*DurableAuditJournal, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("audit journal path is required")
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, errors.New("audit journal path must be a regular file, not a symbolic link")
		}
		if info.Mode().Perm()&0o077 != 0 {
			return nil, errors.New("audit journal permissions must not grant group or other access")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect audit journal: %w", err)
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return nil, fmt.Errorf("create audit journal directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open audit journal: %w", err)
	}
	journal := &DurableAuditJournal{file: file, path: path}
	if err := journal.verifyLocked(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return journal, nil
}

// Record appends and synchronizes one validated decision before returning.
func (journal *DurableAuditJournal) Record(_ context.Context, event DecisionEvent) error {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	if journal.file == nil {
		return errors.New("audit journal is closed")
	}
	if journal.failed != nil {
		return journal.failed
	}
	event.Timestamp = event.Timestamp.UTC()
	if err := event.Validate(); err != nil {
		return err
	}
	sequence, overflow := addOne(journal.lastSequence)
	if overflow {
		journal.failed = errors.New("audit journal sequence exhausted")
		return journal.failed
	}
	document := auditEventDocument(event)
	digest, err := auditRecordDigest(sequence, journal.lastDigest, document)
	if err != nil {
		return err
	}
	record := AuditJournalRecord{
		FormatVersion:  auditJournalFormatVersion,
		Sequence:       strconv.FormatUint(sequence, 10),
		PreviousSHA256: hex.EncodeToString(journal.lastDigest[:]),
		Event:          document,
		RecordSHA256:   hex.EncodeToString(digest[:]),
	}
	encoded, err := marshalCanonicalJSON(record)
	if err != nil {
		return fmt.Errorf("encode audit record: %w", err)
	}
	if len(encoded) > maxAuditRecordBytes {
		return fmt.Errorf("audit record exceeds %d bytes", maxAuditRecordBytes)
	}
	encoded = append(encoded, '\n')
	if _, err := journal.file.Write(encoded); err != nil {
		journal.failed = fmt.Errorf("append audit journal: %w", err)
		return journal.failed
	}
	if err := journal.file.Sync(); err != nil {
		journal.failed = fmt.Errorf("synchronize audit journal: %w", err)
		return journal.failed
	}
	journal.lastSequence = sequence
	journal.lastDigest = digest
	return nil
}

// ReadAuditPage verifies the complete chain before returning a bounded page.
func (journal *DurableAuditJournal) ReadAuditPage(
	_ context.Context,
	afterSequence uint64,
	limit int,
	principal Principal,
) (AuditPage, error) {
	if limit < 1 || limit > maxAuditPageSize {
		return AuditPage{}, fmt.Errorf("audit page size must be between 1 and %d", maxAuditPageSize)
	}
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	if journal.file == nil {
		return AuditPage{}, errors.New("audit journal is closed")
	}
	if journal.failed != nil {
		return AuditPage{}, journal.failed
	}
	records, nextSequence, lastSequence, lastDigest, err := readAndVerifyAuditJournal(
		journal.file,
		afterSequence,
		limit,
		&principal,
	)
	if err != nil {
		journal.failed = err
		return AuditPage{}, err
	}
	if lastSequence != journal.lastSequence || lastDigest != journal.lastDigest {
		journal.failed = errors.New("audit journal changed outside its owning process")
		return AuditPage{}, journal.failed
	}
	return AuditPage{
		Records:      records,
		NextSequence: strconv.FormatUint(nextSequence, 10),
		EndOfJournal: nextSequence == journal.lastSequence,
	}, nil
}

// Close synchronizes and closes the journal.
func (journal *DurableAuditJournal) Close() error {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	if journal.file == nil {
		return nil
	}
	err := errors.Join(journal.file.Sync(), journal.file.Close())
	journal.file = nil
	return err
}

func (journal *DurableAuditJournal) verifyLocked() error {
	records, _, lastSequence, lastDigest, err := readAndVerifyAuditJournal(journal.file, 0, 1, nil)
	_ = records
	if err != nil {
		return fmt.Errorf("verify audit journal %s: %w", journal.path, err)
	}
	journal.lastSequence = lastSequence
	journal.lastDigest = lastDigest
	return nil
}

func readAndVerifyAuditJournal(
	file *os.File,
	afterSequence uint64,
	limit int,
	principal *Principal,
) ([]AuditJournalRecord, uint64, uint64, [sha256.Size]byte, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, 0, zeroAuditDigest, fmt.Errorf("seek audit journal: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, 0, 0, zeroAuditDigest, fmt.Errorf("stat audit journal: %w", err)
	}
	if info.Size() > 0 {
		last := []byte{0}
		if _, err := file.ReadAt(last, info.Size()-1); err != nil || last[0] != '\n' {
			return nil, 0, 0, zeroAuditDigest, errors.New("audit journal ends with a partial record")
		}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, 0, zeroAuditDigest, fmt.Errorf("seek audit journal: %w", err)
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4<<10), maxAuditRecordBytes+1)
	previous := zeroAuditDigest
	var sequence uint64
	nextSequence := afterSequence
	records := make([]AuditJournalRecord, 0, limit)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		record, digest, err := decodeAndVerifyAuditRecord(line, sequence+1, previous)
		if err != nil {
			return nil, 0, 0, zeroAuditDigest, err
		}
		sequence++
		previous = digest
		if sequence > afterSequence && len(records) < limit &&
			(principal == nil || principal.Allows(ActionAuditRead, record.Event.Scope)) {
			records = append(records, record)
			nextSequence = sequence
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, 0, zeroAuditDigest, fmt.Errorf("scan audit journal: %w", err)
	}
	if len(records) < limit {
		nextSequence = sequence
	}
	return records, nextSequence, sequence, previous, nil
}

func decodeAndVerifyAuditRecord(
	encoded []byte,
	expectedSequence uint64,
	expectedPrevious [sha256.Size]byte,
) (AuditJournalRecord, [sha256.Size]byte, error) {
	var record AuditJournalRecord
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return AuditJournalRecord{}, zeroAuditDigest, fmt.Errorf("decode audit record: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return AuditJournalRecord{}, zeroAuditDigest, errors.New("audit record contains trailing data")
	}
	canonical, err := marshalCanonicalJSON(record)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return AuditJournalRecord{}, zeroAuditDigest, errors.New("audit record is not canonical JSON")
	}
	sequence, err := strconv.ParseUint(record.Sequence, 10, 64)
	if err != nil || strconv.FormatUint(sequence, 10) != record.Sequence || sequence != expectedSequence {
		return AuditJournalRecord{}, zeroAuditDigest, errors.New("audit record sequence is invalid")
	}
	if record.FormatVersion != auditJournalFormatVersion ||
		record.PreviousSHA256 != hex.EncodeToString(expectedPrevious[:]) {
		return AuditJournalRecord{}, zeroAuditDigest, errors.New("audit record chain predecessor is invalid")
	}
	event, err := decisionEvent(record.Event)
	if err != nil {
		return AuditJournalRecord{}, zeroAuditDigest, err
	}
	if err := event.Validate(); err != nil {
		return AuditJournalRecord{}, zeroAuditDigest, err
	}
	digest, err := auditRecordDigest(sequence, expectedPrevious, record.Event)
	if err != nil {
		return AuditJournalRecord{}, zeroAuditDigest, err
	}
	if record.RecordSHA256 != hex.EncodeToString(digest[:]) {
		return AuditJournalRecord{}, zeroAuditDigest, errors.New("audit record digest is invalid")
	}
	return record, digest, nil
}

func auditRecordDigest(
	sequence uint64,
	previous [sha256.Size]byte,
	event AuditEventDocument,
) ([sha256.Size]byte, error) {
	encoded, err := marshalCanonicalJSON(event)
	if err != nil {
		return zeroAuditDigest, fmt.Errorf("encode audit event: %w", err)
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("epoch/audit-journal/v1\x00"))
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], sequence)
	_, _ = hasher.Write(number[:])
	_, _ = hasher.Write(previous[:])
	binary.BigEndian.PutUint64(number[:], uint64(len(encoded)))
	_, _ = hasher.Write(number[:])
	_, _ = hasher.Write(encoded)
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return digest, nil
}

func marshalCanonicalJSON(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	encoded := buffer.Bytes()
	if len(encoded) == 0 || encoded[len(encoded)-1] != '\n' {
		return nil, errors.New("canonical JSON encoder omitted its record terminator")
	}
	return append([]byte(nil), encoded[:len(encoded)-1]...), nil
}

func auditEventDocument(event DecisionEvent) AuditEventDocument {
	return AuditEventDocument{
		EventTimeUnixMS:      strconv.FormatInt(event.Timestamp.UTC().UnixMilli(), 10),
		RequestID:            event.RequestID,
		PrincipalID:          event.PrincipalID,
		PolicyID:             event.PolicyID,
		AuthenticationMethod: event.AuthenticationMethod,
		Action:               event.Action,
		Decision:             event.Decision,
		Reason:               event.Reason,
		Scope:                event.Scope,
	}
}

func decisionEvent(document AuditEventDocument) (DecisionEvent, error) {
	eventTimeUnixMS, err := strconv.ParseInt(document.EventTimeUnixMS, 10, 64)
	if err != nil || eventTimeUnixMS <= 0 || strconv.FormatInt(eventTimeUnixMS, 10) != document.EventTimeUnixMS {
		return DecisionEvent{}, errors.New("audit event time must be a canonical positive Unix millisecond")
	}
	timestamp := time.UnixMilli(eventTimeUnixMS).UTC()
	return DecisionEvent{
		Timestamp:            timestamp,
		RequestID:            document.RequestID,
		PrincipalID:          document.PrincipalID,
		PolicyID:             document.PolicyID,
		AuthenticationMethod: document.AuthenticationMethod,
		Action:               document.Action,
		Decision:             document.Decision,
		Reason:               document.Reason,
		Scope:                document.Scope,
	}, nil
}

func addOne(value uint64) (uint64, bool) {
	if value == ^uint64(0) {
		return 0, true
	}
	return value + 1, false
}

// TeeAuditSink commits to primary before emitting the secondary diagnostic.
type TeeAuditSink struct {
	primary interface {
		AuditSink
		AuditReader
	}
	secondary AuditSink
}

// NewTeeAuditSink combines a durable readable journal with a diagnostic sink.
func NewTeeAuditSink(
	primary interface {
		AuditSink
		AuditReader
	},
	secondary AuditSink,
) *TeeAuditSink {
	if primary == nil || secondary == nil {
		panic("auth: nil tee audit sink")
	}
	return &TeeAuditSink{primary: primary, secondary: secondary}
}

func (sink *TeeAuditSink) Record(ctx context.Context, event DecisionEvent) error {
	if err := sink.primary.Record(ctx, event); err != nil {
		return err
	}
	return sink.secondary.Record(ctx, event)
}

func (sink *TeeAuditSink) ReadAuditPage(
	ctx context.Context,
	afterSequence uint64,
	limit int,
	principal Principal,
) (AuditPage, error) {
	return sink.primary.ReadAuditPage(ctx, afterSequence, limit, principal)
}
