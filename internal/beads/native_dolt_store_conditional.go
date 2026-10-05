package beads

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	beadslib "github.com/steveyegge/beads"
	"github.com/steveyegge/beads/issueops"
)

var (
	_ ConditionalWriter                = (*NativeDoltStore)(nil)
	_ AtomicConditionalCloser          = (*NativeDoltStore)(nil)
	_ MetadataCASWriter                = (*NativeDoltStore)(nil)
	_ conditionalWriteCapabilityProber = (*NativeDoltStore)(nil)
)

// CloseWithMetadataIfMatch merges metadata into id and closes it in one
// atomic issueops.BatchApplier request, but only while the exact opaque row
// version still matches.
//
// The role's own MetadataPatch.Set performs the key-level merge the old
// hand-rolled read-modify-write used to do by hand, so the update item only
// ever carries the caller's delta. The one thing that merge cannot give us is
// the CLOSE REASON: tx.CloseIssue always took an explicit reason string, and
// this store's convention stashes it in metadata["close_reason"] rather than
// threading it through every caller as its own argument. So the merged view
// is still computed here, LOCALLY and PURELY for that one string — current
// metadata (from the pre-read) overlaid with the caller's delta, read back
// for "close_reason" — and never written anywhere itself; the actual
// persisted write is the role's own per-key merge below.
//
// ONE REQUEST, TWO ITEMS, ONE FENCE. The update item carries ExpectedVersion;
// the close item does not, because UpdateItem.ExpectedVersion's
// "already-touched" rule (issueops/batchapplier.go) forbids guarding the same
// row twice in one request, and a second guard would be redundant anyway: the
// whole request is one transaction, so once the update's fence passes nothing
// can interleave before the close lands beside it.
func (s *NativeDoltStore) CloseWithMetadataIfMatch(id string, expectedRevision int64, metadata map[string]string) (Bead, error) {
	if err := s.readOnlyGuard(); err != nil {
		return Bead{}, err
	}
	storage, release, err := s.acquireStorage()
	if err != nil {
		return Bead{}, err
	}
	defer release()
	ctx, cancel := nativeDoltOperationContext(context.TODO())
	defer cancel()

	reader, err := storage.IssueReader()
	if err != nil {
		return Bead{}, nativeStoreError(id, err)
	}
	current, err := reader.Get(ctx, issueops.GetRequest{ID: id})
	if err != nil {
		return Bead{}, nativeReadNotFound(id, err)
	}
	if current == nil {
		return Bead{}, fmt.Errorf("bead %q: %w", id, ErrNotFound)
	}

	currentMetadata, err := metadataMapFromNative(current.Metadata)
	if err != nil {
		return Bead{}, fmt.Errorf("parsing metadata for bead %q: %w", id, err)
	}
	merged := make(map[string]string, len(currentMetadata)+len(metadata))
	for key, value := range currentMetadata {
		merged[key] = value
	}
	for key, value := range metadata {
		merged[key] = value
	}
	reason := strings.TrimSpace(merged["close_reason"])

	patch, err := nativeIssuePatchFromUpdateOpts(UpdateOpts{Metadata: metadata})
	if err != nil {
		return Bead{}, fmt.Errorf("conditional close %s: %w", id, err)
	}

	// See UpdateIfMatch/CloseIfMatch: wrap only the checked write so a
	// transient serialization conflict is retried while a version mismatch
	// still short-circuits through conditionalWriteError. reason and patch are
	// deterministic functions of the pre-read issue and the caller's metadata
	// and stay valid across attempts; if another writer actually touched this
	// row in between, the update item's ExpectedVersion refuses the retry
	// instead of silently reusing a stale reason.
	err = retryOnNativeDoltSerializationConflict(func() error {
		applier, err := storage.BatchApplier()
		if err != nil {
			return nativeStoreError(id, err)
		}
		_, err = applier.ApplyBatch(ctx, issueops.ApplyBatchRequest{
			Actor: s.actor,
			Items: []issueops.ApplyItem{
				{
					Kind: issueops.ItemUpdate,
					Update: &issueops.UpdateItem{
						Target:          issueops.Ref{ID: id},
						Patch:           patch,
						ExpectedVersion: &expectedRevision,
					},
				},
				{
					Kind: issueops.ItemClose,
					Close: &issueops.CloseItem{
						Target: issueops.Ref{ID: id},
						Reason: reason,
						// Force: the old path was CloseIssueInTx, which was
						// policy-free -- it had no blocker/open-child refusal
						// to bypass. Every other close this store issues
						// (e.g. CloseAll) also forces, so this is a direct
						// behavior match, not a new allowance: without it, a
						// parent with an open child would newly refuse a
						// close that used to succeed unconditionally.
						Force: true,
					},
				},
			},
			Provenance: fmt.Sprintf("gc: fenced metadata close bead %s", id),
		})
		return err
	})
	if err != nil {
		return Bead{}, s.conditionalWriteError(ctx, storage, id, expectedRevision, err)
	}

	final, err := reader.Get(ctx, issueops.GetRequest{ID: id})
	if err != nil {
		return Bead{}, nativeReadNotFound(id, err)
	}
	if final == nil {
		return Bead{}, fmt.Errorf("bead %q: %w", id, ErrNotFound)
	}
	if final.Status != beadslib.StatusClosed {
		return Bead{}, fmt.Errorf("closing bead %q atomically: transaction returned status %q", id, final.Status)
	}
	return beadFromNativeIssueDetails(final)
}

// probeConditionalWriteCapability reports whether the native backend actually
// exposes every issueops role the conditional-write cluster depends on:
// IssueLifecycle (UpdateIfMatch/CloseIfMatch), Deleter (DeleteIfMatch),
// BatchApplier (the assignee/status update door and CloseWithMetadataIfMatch)
// and IssueReader (the pre-reads CloseIfMatch/CloseWithMetadataIfMatch need
// for their close reason). A storage value that answers acquireStorage but
// refuses one of these roles with *beads.ErrUnsupported is NOT conditional-write
// capable, and this probe must say so rather than claim unconditional support.
func (s *NativeDoltStore) probeConditionalWriteCapability() (bool, string) {
	storage, release, err := s.acquireStorage()
	if err != nil {
		return false, err.Error()
	}
	defer release()
	if _, err := storage.IssueLifecycle(); err != nil {
		return false, err.Error()
	}
	if _, err := storage.Deleter(); err != nil {
		return false, err.Error()
	}
	if _, err := storage.BatchApplier(); err != nil {
		return false, err.Error()
	}
	if _, err := storage.IssueReader(); err != nil {
		return false, err.Error()
	}
	return true, "native beads backend exposes the issueops lifecycle, deleter, batch-apply and reader roles"
}

// UpdateIfMatch applies row-backed opts only while id still has
// expectedRevision.
func (s *NativeDoltStore) UpdateIfMatch(id string, expectedRevision int64, opts UpdateOpts) error {
	if err := s.readOnlyGuard(); err != nil {
		return err
	}
	if err := validateConditionalUpdateOpts(opts); err != nil {
		return fmt.Errorf("conditional update %s: %w", id, err)
	}
	storage, release, err := s.acquireStorage()
	if err != nil {
		return err
	}
	defer release()
	ctx, cancel := nativeDoltOperationContext(context.TODO())
	defer cancel()

	patch, err := nativeIssuePatchFromUpdateOpts(opts)
	if err != nil {
		return fmt.Errorf("conditional update %s: %w", id, err)
	}

	// Retry a transient native-Dolt serialization conflict rather than letting
	// it escape raw: embedded-Dolt has no internal withRetryTx, and the
	// nudge-queue CAS loop only re-drives PreconditionFailedError, so an
	// un-retried conflict would hard-fail to an API 500. The fence is
	// unaffected — ExpectedVersion is re-checked every attempt and a genuine
	// mismatch returns issueops.ErrVersionMismatch, which is not a
	// serialization conflict, so precondition failures still propagate
	// immediately (never retried). Mirrors DeleteIfMatch/CloseWithMetadataIfMatch.
	//
	// See updateOnceThroughFacade: an assignee or status edit needs a force
	// waiver neither the plain Lifecycle door nor its wire counterpart
	// publishes, so it takes the single-item batch door instead, carrying
	// ExpectedVersion exactly as the plain door does. validateConditionalUpdateOpts
	// has already rejected ParentID/Labels, so that is the only split this
	// door needs.
	err = retryOnNativeDoltSerializationConflict(func() error {
		if opts.Assignee != nil || opts.Status != nil {
			applier, err := storage.BatchApplier()
			if err != nil {
				return nativeStoreError(id, err)
			}
			_, err = applier.ApplyBatch(ctx, issueops.ApplyBatchRequest{
				Actor: s.actor,
				Items: []issueops.ApplyItem{{
					Kind: issueops.ItemUpdate,
					Update: &issueops.UpdateItem{
						Target:                issueops.Ref{ID: id},
						Patch:                 patch,
						ExpectedVersion:       &expectedRevision,
						ForceAssigneeTransfer: opts.Assignee != nil,
						ForceClosePolicy:      opts.Status != nil,
					},
				}},
				Provenance: "gc: conditional update " + id,
			})
			return err
		}
		ops, err := storage.IssueLifecycle()
		if err != nil {
			return nativeStoreError(id, err)
		}
		_, err = ops.Update(ctx, issueops.UpdateRequest{
			Actor:           s.actor,
			IssueID:         id,
			Patch:           patch,
			ExpectedVersion: &expectedRevision,
		})
		return err
	})
	return s.conditionalWriteError(ctx, storage, id, expectedRevision, err)
}

// CloseIfMatch closes id only while it still has expectedRevision.
func (s *NativeDoltStore) CloseIfMatch(id string, expectedRevision int64) error {
	if err := s.readOnlyGuard(); err != nil {
		return err
	}
	storage, release, err := s.acquireStorage()
	if err != nil {
		return err
	}
	defer release()
	ctx, cancel := nativeDoltOperationContext(context.TODO())
	defer cancel()

	reader, err := storage.IssueReader()
	if err != nil {
		return nativeStoreError(id, err)
	}
	current, err := reader.Get(ctx, issueops.GetRequest{ID: id})
	if err != nil {
		return nativeReadNotFound(id, err)
	}
	if current == nil {
		return fmt.Errorf("bead %q: %w", id, ErrNotFound)
	}
	reason := nativeCloseReasonFromIssue(&current.Issue)

	// See UpdateIfMatch: wrap only the checked write so a transient
	// serialization conflict is retried while a version mismatch still
	// short-circuits through conditionalWriteError. The pre-read close reason is
	// a deterministic function of the issue and stays valid across attempts.
	err = retryOnNativeDoltSerializationConflict(func() error {
		lifecycle, err := storage.IssueLifecycle()
		if err != nil {
			return nativeStoreError(id, err)
		}
		_, err = lifecycle.Close(ctx, issueops.CloseRequest{
			Actor:           s.actor,
			IssueID:         id,
			Reason:          reason,
			ExpectedVersion: &expectedRevision,
		})
		return err
	})
	return s.conditionalWriteError(ctx, storage, id, expectedRevision, err)
}

// DeleteIfMatch deletes id only while it still has expectedRevision.
//
// ROUTING THIS THROUGH issueops.Deleter CHANGED ONE OBSERVABLE BEHAVIOR,
// DELIBERATELY: the role rewrites every surviving GRAPH NEIGHBOR's text that
// cites the deleted id to `[deleted:<id>]` (Deleter.Delete's doc), bumping
// each rewritten neighbor's own revision, inside the SAME transaction as the
// delete. The raw tx.DeleteIssue this replaced had no such rewrite — a
// neighbor's description, notes, design or acceptance criteria kept citing a
// since-deleted id verbatim, and its revision never moved.
//
// issueops.DeleteRequest has no flag to ask for the old behavior (checked
// against the pinned beads v1.3.1: DeleteRequest carries Actor, IDs,
// ExpectedVersion, Cascade, Force and DryRun, and none of them is "skip the
// rewrite" — see issueops.Deleter's doc, "WHICH ROWS GET REWRITTEN"). This is
// kept anyway, as a DELIBERATE alignment with bd's own delete semantics
// (bd delete has always rewritten citing neighbors this way; this store is
// the one route that had not caught up), pinned by
// TestNativeDoltStoreDeleteIfMatchRewritesNeighborTextThroughFacade. A beads
// follow-up to add a Deleter option that opts out of the rewrite, for a
// caller that genuinely wants the old no-rewrite delete, is open as a
// request to the coordinator rather than something this slice can add
// upstream itself.
func (s *NativeDoltStore) DeleteIfMatch(id string, expectedRevision int64) error {
	if err := s.readOnlyGuard(); err != nil {
		return err
	}
	storage, release, err := s.acquireStorage()
	if err != nil {
		return err
	}
	defer release()
	ctx, cancel := nativeDoltOperationContext(context.TODO())
	defer cancel()

	err = retryOnNativeDoltSerializationConflict(func() error {
		deleter, err := storage.Deleter()
		if err != nil {
			return nativeStoreError(id, err)
		}
		// Force:true, no Cascade: see deleteIDs's identical precedent -- the
		// raw tx.DeleteIssue this replaced applied no dependents guard at
		// all, so Cascade would go further than the old behavior and delete
		// rows the caller never named.
		_, err = deleter.Delete(ctx, issueops.DeleteRequest{
			Actor:           s.actor,
			IDs:             []string{id},
			ExpectedVersion: &expectedRevision,
			Force:           true,
		})
		return err
	})
	if err != nil {
		return s.conditionalWriteError(ctx, storage, id, expectedRevision, err)
	}
	if err := s.localStrings.DeleteBead(id); err != nil {
		return fmt.Errorf("deleting bead %q: cleaning up local strings: %w", id, err)
	}
	return nil
}

// TransferIfCurrent moves an in_progress bead from one exact assignee
// spelling to another, only while the bead still carries fromAssignee. See
// BdStore.TransferIfCurrent (bdstore_conditional_release.go) for the
// CLI-facing contract this matches minus the shell mechanics: same
// trim-and-require-both-nonempty precondition, same fromAssignee==toAssignee
// short-circuit, same (true/false, nil) "someone else holds it, or it is no
// longer in progress" outcome on a lost precondition, same readback-decides
// handling of a retried write whose first attempt already committed.
//
// THE ROUTE DIFFERS FROM UpdateIfMatch'S ASSIGNEE BRANCH ON PURPOSE.
// issueops.UpdateRequest.ExpectedAssignee is documented as authorizing the
// Patch.Assignee transfer BY ITSELF ("A match authorizes the requested
// Patch.Assignee transfer: this compare-and-set replaces the ordinary
// anti-steal fence") and ForceAssigneeTransfer "must be false ... when
// [ExpectedAssignee] is non-nil" -- the two are mutually exclusive, not
// complementary. So this goes through the plain storage.IssueLifecycle().Update
// door with ExpectedAssignee/ExpectedStatus set, never the force-waiver batch
// door UpdateIfMatch's assignee branch needs (that door has no
// ExpectedAssignee/ExpectedStatus of its own to offer).
//
// THERE IS NO BD-VERSION-SKEW CASE HERE. BdStore.TransferIfCurrent reports
// ErrConditionalTransferUnsupported when the bd binary on PATH predates the
// --if-assignee/--if-status flags; a native backend has no such external
// binary to be too old, so this method has nothing analogous to return. Any
// operational error the role surfaces beyond the two precondition sentinels
// and ErrNotFound is wrapped and returned as a genuine error, same as every
// other conditional method in this file.
func (s *NativeDoltStore) TransferIfCurrent(id, fromAssignee, toAssignee string) (bool, error) {
	// The read-only latch is checked FIRST, before any validation or the
	// same-assignee short circuit: see
	// TestNativeDoltStoreReadOnlyLatchRefusesEveryMutationWithoutTouchingStorage,
	// which drives this with zero-value args and requires ErrProxiedNativeReadOnly
	// specifically, not a validation error that happens to also refuse.
	if err := s.readOnlyGuard(); err != nil {
		return false, err
	}
	fromAssignee = strings.TrimSpace(fromAssignee)
	toAssignee = strings.TrimSpace(toAssignee)
	if fromAssignee == "" || toAssignee == "" {
		return false, fmt.Errorf("bead %q: transfer-if-current: from and to assignees are required", id)
	}
	if fromAssignee == toAssignee {
		return true, nil
	}
	storage, release, err := s.acquireStorage()
	if err != nil {
		return false, err
	}
	defer release()
	ctx, cancel := nativeDoltOperationContext(context.TODO())
	defer cancel()

	expectedAssignee := fromAssignee
	expectedStatus := issueops.StatusInProgress
	err = retryOnNativeDoltSerializationConflict(func() error {
		ops, err := storage.IssueLifecycle()
		if err != nil {
			return nativeStoreError(id, err)
		}
		_, err = ops.Update(ctx, issueops.UpdateRequest{
			Actor:   s.actor,
			IssueID: id,
			Patch: issueops.IssuePatch{
				Assignee: issueops.Field[string]{Set: true, Value: toAssignee},
			},
			ExpectedAssignee: &expectedAssignee,
			ExpectedStatus:   &expectedStatus,
			Provenance:       fmt.Sprintf("gc: transfer-if-current bead %s", id),
		})
		return err
	})
	if err == nil {
		return true, nil
	}
	if errors.Is(err, issueops.ErrAssigneeMismatch) || errors.Is(err, issueops.ErrStatusMismatch) {
		// Either another claimant holds it (or it is no longer in_progress), or
		// a retried attempt already committed this very transfer -- only the
		// readback can tell them apart. Any readback failure (role unsupported,
		// a vanished row) falls through to the "someone else/no longer
		// eligible" (false, nil) answer rather than escalating to an error:
		// the precondition genuinely missed either way.
		if reader, readerErr := storage.IssueReader(); readerErr == nil {
			if current, getErr := reader.Get(ctx, issueops.GetRequest{ID: id}); getErr == nil && current != nil {
				if strings.TrimSpace(current.Assignee) == toAssignee {
					return true, nil
				}
			}
		}
		return false, nil
	}
	if errors.Is(err, issueops.ErrNotFound) {
		return false, nil
	}
	return false, fmt.Errorf("native transfer-if-current %s: %w", id, err)
}

// conditionalWriteError classifies a role refusal for the four conditional
// methods above.
//
// issueops.ErrVersionMismatch -- NOT beadslib.ErrVersionMismatch, a
// different sentinel from the internal storage package that the raw
// UpdateIssueChecked/CloseIssueChecked/RunInTransaction paths this replaced
// used to return -- is the role's version-fence refusal, matched through
// errors.Is whether err is the bare sentinel or a *issueops.ItemError
// wrapping it (ItemError.Unwrap returns the inner sentinel, so errors.Is
// reaches it either way; the batch door in UpdateIfMatch and
// CloseWithMetadataIfMatch both return it wrapped this way).
func (s *NativeDoltStore) conditionalWriteError(
	ctx context.Context,
	storage beadslib.Storage,
	id string,
	expectedRevision int64,
	err error,
) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, issueops.ErrNotFound) {
		return fmt.Errorf("bead %q: %w: %w", id, ErrNotFound, err)
	}
	if !errors.Is(err, issueops.ErrVersionMismatch) {
		return nativeStoreError(id, err)
	}
	current := int64(0)
	if reader, readerErr := storage.IssueReader(); readerErr == nil {
		if issue, readErr := reader.Get(ctx, issueops.GetRequest{ID: id}); readErr == nil && issue != nil {
			current = issue.RowVersion
		}
	}
	return &PreconditionFailedError{
		ID:       id,
		Expected: expectedRevision,
		Current:  current,
		Raw:      err.Error(),
	}
}

// CompareAndSetMetadataKey atomically sets metadata[key] = next when the key's
// current value equals expected.
//
// expected == "" matches a key that is ABSENT or present with the empty value:
// parsing an absent key out of the stored metadata map yields "", so the two
// states are indistinguishable here exactly as they are to callers (release
// paths write "" to clear). Returns (true, nil) on swap, (false, nil) on a
// genuine value mismatch — a lost race is NOT an error — and (false, err) for
// a missing bead, a malformed metadata blob, or a transport failure.
//
// ATOMICITY IS THE ROLE'S. issueops.MetadataCAS reads the key, compares it and
// re-serializes the whole metadata object inside ONE transaction the substrate
// opens, which is the property this method used to compose by hand out of
// RunInTransaction. Composing it by hand is no longer possible everywhere:
// RunInTransaction is off the v0 served surface, so the hand-built version was
// a hard failure on any store reached over the wire. The role is a required
// member of the Storage contract, so every backend answers it.
//
// THE STRING FRONT DOOR AND THE ROLE DISAGREE ABOUT TWO THINGS, and both
// disagreements are normalized here rather than pushed onto callers:
//
//   - ABSENT vs PRESENT-EMPTY. The role tells them apart (a nil Expected means
//     absent; an Expected of `""` means present holding the empty string); this
//     front door does not, and its conformance contract says an empty
//     expectation claims EITHER. So an empty expectation is TWO ARMS: absent
//     first, then present-empty, and the second is dialed only when the first
//     refusal reports Current as exactly the empty string. Any other Current is
//     a live value and a genuine lost race, which must not be retried against —
//     a single-arm request would claim only one of the two states, and which
//     one it missed would depend on whether the row predates the
//     clear-as-deletion below.
//   - CLEARING. A next of "" is spelled as the role's DELETION (a nil Value)
//     rather than as a stored empty string. The two are observationally
//     identical through this front door — TestMetadataEmptyStringClearContract
//     pins the observable clear semantics and explicitly does not assert
//     physical deletion — and deletion is the spelling that leaves the NEXT
//     acquire winning on its first arm instead of putting every workspace
//     permanently on the fallback.
func (s *NativeDoltStore) CompareAndSetMetadataKey(id, key, expected, next string) (bool, error) {
	if err := s.readOnlyGuard(); err != nil {
		return false, err
	}
	storage, release, err := s.acquireStorage()
	if err != nil {
		return false, err
	}
	defer release()
	ctx, cancel := nativeDoltOperationContext(context.TODO())
	defer cancel()

	cas, err := storage.MetadataCAS()
	if err != nil {
		return false, nativeStoreError(id, err)
	}
	value, err := nativeMetadataCASValue(next)
	if err != nil {
		return false, fmt.Errorf("compare-and-set metadata on %q: %w", id, err)
	}
	request := issueops.CompareAndSetKeyRequest{
		Actor:   s.actor,
		IssueID: id,
		Key:     key,
		Value:   value,
	}

	if expected != "" {
		stringExpected, err := nativeMetadataCASValue(expected)
		if err != nil {
			return false, fmt.Errorf("compare-and-set metadata on %q: %w", id, err)
		}
		request.Expected = stringExpected
		result, err := cas.CompareAndSetKey(ctx, request)
		if err != nil {
			return false, nativeStoreError(id, err)
		}
		if result.Swapped {
			return true, nil
		}
		// Arm two, dialed only when the refusal's Current is a non-string JSON
		// scalar whose own text equals expected -- a stored value this store's
		// writers never produce (they only ever write strings) but that a
		// caller still names by its plain text, the way metadataMapFromNative's
		// map[string]string rendering does (a non-string value renders as its
		// own JSON text). Retrying with the stored bytes verbatim is the one
		// extra call that lets the comparison reach them. Anything else -- a
		// live different value, or a JSON string (whose text is quoted and so
		// never equals the unquoted expected, already covered by arm one) -- is
		// a genuine mismatch, and retrying against it would turn a lost race
		// into a steal.
		raw, ok := nativeMetadataCASNonStringTextMatch(result.Current, expected)
		if !ok {
			return false, nil
		}
		request.Expected = raw
		result, err = cas.CompareAndSetKey(ctx, request)
		if err != nil {
			return false, nativeStoreError(id, err)
		}
		return result.Swapped, nil
	}

	// Arm one: the key is absent.
	request.Expected = nil
	result, err := cas.CompareAndSetKey(ctx, request)
	if err != nil {
		return false, nativeStoreError(id, err)
	}
	if result.Swapped {
		return true, nil
	}
	// Arm two, and ONLY when the refusal named the empty string. A refusal
	// carrying any other value is a live holder, and retrying against it would
	// turn a lost race into a steal.
	if !nativeMetadataCASIsEmptyString(result.Current) {
		return false, nil
	}
	empty := json.RawMessage(`""`)
	request.Expected = &empty
	result, err = cas.CompareAndSetKey(ctx, request)
	if err != nil {
		return false, nativeStoreError(id, err)
	}
	return result.Swapped, nil
}

// nativeMetadataCASValue encodes one string-shaped metadata value for the role.
// The empty string is the CLEAR, and the role spells a clear as a nil value.
func nativeMetadataCASValue(value string) (*json.RawMessage, error) {
	if value == "" {
		return nil, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encoding metadata value: %w", err)
	}
	raw := json.RawMessage(encoded)
	return &raw, nil
}

// nativeMetadataCASNonStringTextMatch reports whether current is a stored raw
// JSON value that is NOT itself a JSON string but whose text equals expected
// once surrounding whitespace is trimmed -- production carries beads with a
// non-string metadata value written by some other path (an integer
// gc.control_epoch, among others), and this is the one comparison that still
// reaches them without a pre-read: the role's own refusal already carries the
// stored value in Current, so there is nothing to fetch separately.
//
// A JSON string's text is quoted and so never equals the unquoted expected --
// that shape is already handled by arm one's plain string encoding, and
// double-matching it here would risk a false positive against a string that
// happens to equal its own quoted form.
func nativeMetadataCASNonStringTextMatch(current *json.RawMessage, expected string) (*json.RawMessage, bool) {
	if current == nil {
		return nil, false
	}
	var asString string
	if err := json.Unmarshal(*current, &asString); err == nil {
		return nil, false
	}
	if strings.TrimSpace(string(*current)) != expected {
		return nil, false
	}
	raw := append(json.RawMessage(nil), *current...)
	return &raw, true
}

// nativeMetadataCASIsEmptyString reports the one Current value that makes the
// present-empty arm worth dialing. A nil Current is an absent key the first arm
// already lost to (a concurrent writer took it between the two), and anything
// else is a live value.
func nativeMetadataCASIsEmptyString(current *json.RawMessage) bool {
	if current == nil {
		return false
	}
	var decoded string
	if err := json.Unmarshal(*current, &decoded); err != nil {
		return false
	}
	return decoded == ""
}
