package beads

import "sort"

// This file holds the CachingStore's bead.closed announcement bookkeeping: the
// queue of closes a non-announcing absorb installed (gastownhall/gascity#6860)
// and the drain that turns them into bead.closed notifications exactly once.

// trackCloseTransitionLocked keeps unannouncedCloses in step with the row just
// installed for id. A row that is not closed cancels any queued close (a
// reopen before the drain must not announce a stale close). A not-closed to
// closed transition is queued unless the caller announces it itself, and so is
// a closed row installed with no cached row before it while a closing write of
// ours is in flight (closeIntents). Caller must hold c.mu in write mode.
func (c *CachingStore) trackCloseTransitionLocked(id string, previous Bead, hadPrevious bool, installed Bead, announced bool) {
	if installed.Status != "closed" || announced {
		c.dropQueuedCloseLocked(id)
		return
	}
	switch {
	case hadPrevious && previous.Status != "closed":
		// A close observed over a cached open row.
	case !hadPrevious && c.closeIntents[id] != nil && !c.closeIntents[id].announced:
		// A close a write of ours is making, observed before that write
		// claims it: queue it so the claim (or the drain) announces it.
	default:
		return
	}
	if c.unannouncedCloses == nil {
		c.unannouncedCloses = make(map[string]Bead)
	}
	c.unannouncedCloses[id] = cloneBead(installed)
	c.hasUnannouncedCloses.Store(true)
}

// dropQueuedCloseLocked removes id's queued close and reports whether one was
// queued. Caller must hold c.mu in write mode.
func (c *CachingStore) dropQueuedCloseLocked(id string) bool {
	if _, queued := c.unannouncedCloses[id]; !queued {
		return false
	}
	delete(c.unannouncedCloses, id)
	c.hasUnannouncedCloses.Store(len(c.unannouncedCloses) > 0)
	return true
}

// takeUnannouncedClosesLocked drains the queued closes in id order. Caller
// must hold c.mu in write mode.
func (c *CachingStore) takeUnannouncedClosesLocked() []Bead {
	if len(c.unannouncedCloses) == 0 {
		return nil
	}
	ids := make([]string, 0, len(c.unannouncedCloses))
	for id := range c.unannouncedCloses {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	closed := make([]Bead, 0, len(ids))
	for _, id := range ids {
		closed = append(closed, c.unannouncedCloses[id])
		c.noteCloseAnnouncedLocked(id)
	}
	c.unannouncedCloses = nil
	c.hasUnannouncedCloses.Store(false)
	return closed
}

// announceUnannouncedCloses emits bead.closed for every close queued by
// trackCloseTransitionLocked. Draining under c.mu makes each queued close go
// out exactly once however many goroutines race here. The close was inferred
// from a read, so it carries ChangeScan's inferred source. Callers must not
// hold c.mu.
func (c *CachingStore) announceUnannouncedCloses() {
	if !c.hasUnannouncedCloses.Load() {
		return
	}
	c.mu.Lock()
	closed := c.takeUnannouncedClosesLocked()
	c.mu.Unlock()
	for _, b := range closed {
		c.emitChange(ChangeRefresh, "bead.closed", b)
	}
}

// claimCloseLocked decides whether a write that is about to announce id's close
// owns that bead.closed, and must be called before the write installs its row.
// It owns it when it cancels a close a read queued but has not drained yet, or
// when the cached row it replaces is not closed. A cached row that is already
// closed and not queued was announced by whoever installed it, so the write
// does not announce again. With no cached row, uncachedOwns decides: a write
// that itself closed the bead owns its close; one that merely refreshed an
// uncached row does not. Caller must hold c.mu in write mode.
func (c *CachingStore) claimCloseLocked(id string, uncachedOwns bool) bool {
	if c.dropQueuedCloseLocked(id) {
		return true
	}
	previous, held := c.beads[id]
	if !held {
		return uncachedOwns
	}
	return previous.Status != "closed"
}

// updateEventTypeLocked names the event a write that installs installed for id
// announces: bead.closed when it leaves the bead closed and owns that close
// (claimCloseLocked), else bead.updated. An update that leaves the bead closed
// when it was not closed before is a close, whatever spelling asked for it, so
// it is announced as Close announces it; announcing it as bead.updated hid it
// from every bead.closed consumer (gastownhall/gascity#6860). With no cached
// row to compare, the update's own status=closed is the evidence. A close
// announced elsewhere while the update was in flight (announcedElsewhere, from
// its close intent) is not claimed. Caller must hold c.mu in write mode, before
// installing the row.
func (c *CachingStore) updateEventTypeLocked(id string, installed Bead, opts UpdateOpts, announcedElsewhere bool) string {
	if installed.Status != "closed" || announcedElsewhere {
		return "bead.updated"
	}
	if c.claimCloseLocked(id, updateCloses(opts)) {
		c.noteCloseAnnouncedLocked(id)
		return "bead.closed"
	}
	return "bead.updated"
}

// closeIntent is the in-flight state of the closing writes for one bead.
type closeIntent struct {
	// writers counts the closing writes between their backing write and
	// their claim.
	writers int
	// announced records that something else announced the bead's close while
	// the writes were in flight: a drained queue entry, a reconcile pass or
	// RefreshRow that evicted the row as closed, an applied bead.closed event,
	// or another closing write's claim. A writer's claim then declines.
	announced bool
}

// beginCloseIntent registers a closing write for id before its backing write
// (closeIntents). The write must end it with claimCloseIntentLocked, or with
// endCloseIntent when it fails before claiming.
func (c *CachingStore) beginCloseIntent(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeIntents == nil {
		c.closeIntents = make(map[string]*closeIntent)
	}
	intent := c.closeIntents[id]
	if intent == nil {
		intent = &closeIntent{}
		c.closeIntents[id] = intent
	}
	intent.writers++
}

// endCloseIntent ends a closing write that will not claim (its backing write
// failed).
func (c *CachingStore) endCloseIntent(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.endCloseIntentLocked(id)
}

// endCloseIntentLocked drops one in-flight closing write for id and reports
// whether something else announced the close while it was in flight. Caller
// must hold c.mu in write mode.
func (c *CachingStore) endCloseIntentLocked(id string) (announcedElsewhere bool) {
	intent := c.closeIntents[id]
	if intent == nil {
		return false
	}
	announcedElsewhere = intent.announced
	intent.writers--
	if intent.writers <= 0 {
		delete(c.closeIntents, id)
	}
	return announcedElsewhere
}

// noteCloseAnnouncedLocked records that id's close was announced by something
// other than its in-flight closing writes, so their claims decline. It is a
// no-op when no closing write is in flight. Caller must hold c.mu in write
// mode.
func (c *CachingStore) noteCloseAnnouncedLocked(id string) {
	if intent := c.closeIntents[id]; intent != nil {
		intent.announced = true
	}
}

// claimCloseIntentLocked ends a closing write's intent and claims its close
// (claimCloseLocked) under the same lock, so no read can queue the close
// between the two. A close something else announced while the write was in
// flight is not claimed; a claimed close is noted for any other closing write
// of the same bead still in flight. Caller must hold c.mu in write mode.
func (c *CachingStore) claimCloseIntentLocked(id string, uncachedOwns bool) bool {
	if c.endCloseIntentLocked(id) {
		c.dropQueuedCloseLocked(id)
		return false
	}
	own := c.claimCloseLocked(id, uncachedOwns)
	if own {
		c.noteCloseAnnouncedLocked(id)
	}
	return own
}

// updateCloses reports whether opts writes status=closed.
func updateCloses(opts UpdateOpts) bool {
	return opts.Status != nil && *opts.Status == "closed"
}
