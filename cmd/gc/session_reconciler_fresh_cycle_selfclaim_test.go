package main

// Reproductions for ga-81yg38 / ruling ga-pvjbx3 (Q1 + Q2): the #6518
// fresh-cycle guard still kills rig-scoped fresh-mode sessions that have
// already self-claimed their next work bead, or that get cycled onto an
// anchor that is already closed on a live read. Each test encodes one live
// kill shape from 2026-09-25 (supervisor.log) and asserts the behavior the
// ga-pvjbx3 ruling describes. Every test here FAILS on origin/main 0f41de797e.
//
// This is a deliberate SUBSET of the shared repro suite at
// /var/tmp/ga-81yg38-artifacts/session_reconciler_fresh_cycle_guard_repro_test.go.
// That suite also has 3 row-A/C tests (TestFreshCycleRepro_OpenPreviousBeadInRigStoreDefers,
// TestFreshCycleRepro_IncarnationStartedAfterRealCloseDefers,
// TestFreshCycleRepro_IncarnationStartedAfterRealCloseInRigStoreDefers) that
// are owned by sibling bead ga-2weagw (branch builder/ga-2weagw, reviewed,
// not yet merged to main) — its fix, not this one's, is what makes those
// pass. Adding them here would make them diff-owned tests this branch cannot
// pass without duplicating ga-2weagw's out-of-scope work, so per the ruling's
// own exit contract ("Out of scope for this bead: ga-2weagw's own
// store/closed_at fixes") they are left to that branch. Whichever of the two
// branches lands second rebases onto the first, per ga-pvjbx3's Coordination
// section.
//
// The shared helpers below are deliberately renamed from the artifact's names
// (freshCycleReproEnv -> freshCycleSelfClaimReproEnv, reproMemStore ->
// selfClaimReproMemStore, etc.) rather than copied verbatim: ga-2weagw's own
// 3 tests plausibly draw on the same shared artifact file, and two files in
// this package each declaring an unprefixed `func reproMemStore()` would be a
// duplicate-symbol compile error the moment both branches land on main.
import (
	"context"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
)

// freshCycleSelfClaimReproEnv builds the fresh-mode named "witness" session the
// existing #6518 tests use, plus a "gascity" rig (prefix "ga") so work beads
// can live in a rig store exactly as every ga-* bead does in production.
func freshCycleSelfClaimReproEnv(t *testing.T, sessionMeta map[string]string) (*restartRequestTestEnv, beads.Bead, string) {
	t.Helper()
	env := newRestartRequestTestEnv()
	env.cfg = &config.City{
		Workspace:     config.Workspace{Name: "test-city"},
		Rigs:          []config.Rig{{Name: "gascity", Prefix: "ga"}},
		Agents:        []config.Agent{{Name: "witness", StartCommand: "true", MaxActiveSessions: restartRequestTestIntPtr(1)}},
		NamedSessions: []config.NamedSession{{Template: "witness", Mode: "on_demand"}},
	}
	sessionName := config.NamedSessionRuntimeName(env.cfg.Workspace.Name, env.cfg.Workspace, "witness")
	env.desiredState[sessionName] = TemplateParams{
		Command:      "true",
		SessionName:  sessionName,
		TemplateName: "witness",
		ResolvedProvider: &config.ResolvedProvider{
			SessionIDFlag: "--session-id",
		},
	}
	session := env.createSessionBead(sessionName)
	meta := map[string]string{
		namedSessionMetadataKey:      "true",
		namedSessionIdentityMetadata: "witness",
		namedSessionModeMetadata:     "on_demand",
		"template":                   "witness",
		"state":                      "active",
		"wake_mode":                  "fresh",
		"session_key":                "conversation-A",
	}
	for k, v := range sessionMeta {
		meta[k] = v
	}
	env.setSessionMetadata(&session, meta)
	if err := env.sp.Start(context.Background(), sessionName, runtime.Config{Command: "true"}); err != nil {
		t.Fatalf("start session: %v", err)
	}
	if err := env.sp.SetMeta(sessionName, "GC_SESSION_ID", session.ID); err != nil {
		t.Fatalf("SetMeta(GC_SESSION_ID): %v", err)
	}
	return env, session, sessionName
}

func selfClaimReproMemStore() *beads.MemStore {
	s := beads.NewMemStore()
	s.HonorExplicitIDs = true
	return s
}

// selfClaimReproCreate creates b and then applies its status: MemStore.Create
// stores every new bead as "open" whatever Status says, so an in_progress
// claim must be written the way a real claim writes it, as an update.
func selfClaimReproCreate(t *testing.T, s beads.Store, b beads.Bead) {
	t.Helper()
	if _, err := s.Create(b); err != nil {
		t.Fatalf("creating %s: %v", b.ID, err)
	}
	if b.Status != "" && b.Status != "open" {
		status := b.Status
		if err := s.Update(b.ID, beads.UpdateOpts{Status: &status}); err != nil {
			t.Fatalf("setting %s status %s: %v", b.ID, status, err)
		}
	}
}

// selfClaimReproCloseForReal closes id through the store's own Close — the
// path `bd close` takes. Unlike a hand-seeded fixture it does NOT set
// metadata closed_at: real work beads never carry that key (bd keeps
// closed_at as a top-level column, and neither bdIssue nor beads.Bead decodes
// it).
func selfClaimReproCloseForReal(t *testing.T, s beads.Store, id string) {
	t.Helper()
	if err := s.Close(id); err != nil {
		t.Fatalf("closing %s: %v", id, err)
	}
}

// reconcileFreshCycleSelfClaimRepro mirrors reconcileSessionBeadsWithAssignedWork
// but threads rig stores through, the way city_runtime.go's production tick does.
func reconcileFreshCycleSelfClaimRepro(env *restartRequestTestEnv, sessions, assignedWork []beads.Bead, rigStores map[string]beads.Store) {
	poolDesired := make(map[string]int)
	for _, tp := range env.desiredState {
		if tp.TemplateName != "" {
			poolDesired[tp.TemplateName]++
		}
	}
	cfgNames := configuredSessionNames(env.cfg, "", env.store)
	_ = reconcileSessionBeadsAtPath(
		context.Background(), "", sessions, env.desiredState, cfgNames, env.cfg, env.sp, env.store,
		nil, assignedWork, rigStores, nil, env.dt, poolDesired, false, nil, "", nil,
		env.clk, env.rec, 0, 0, &env.stdout, &env.stderr,
	)
}

func assertSelfClaimNotCycled(t *testing.T, env *restartRequestTestEnv, session beads.Bead, sessionName, why string) {
	t.Helper()
	if !env.sp.IsRunning(sessionName) {
		t.Fatalf("session was killed by fresh-cycle; want it left running: %s\nstdout: %s", why, env.stdout.String())
	}
	got, _ := env.store.Get(session.ID)
	if got.Metadata["session_key"] != "conversation-A" {
		t.Fatalf("session_key = %q, want conversation-A preserved (no cycle): %s", got.Metadata["session_key"], why)
	}
}

// Cases 2, 3 and 5 (gascity--investigator). The session worked the previous
// bead, closed it, and claimed its next bead itself with `bd update --claim`
// (the investigator prompt's documented claim path). That path never writes
// current_claim_bead_id, so row E (self-claimed) cannot fire, and rows D/F
// cycle it. Q1's existence check finds the newly-claimed bead directly
// (in_progress, assigned to this session) without ever consulting
// current_claim_bead_id.
func TestFreshCycleRepro_BdClaimedNextBeadIsSelfClaim(t *testing.T) {
	env, session, sessionName := freshCycleSelfClaimReproEnv(t, map[string]string{
		"awake_started_at": time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano),
	})
	rig := selfClaimReproMemStore()
	selfClaimReproCreate(t, rig, beads.Bead{ID: "ga-prev", Title: "previous investigation", Type: "task", Status: "in_progress", Assignee: "witness"})
	selfClaimReproCloseForReal(t, rig, "ga-prev")
	env.setSessionMetadata(&session, map[string]string{sessionpkg.CurrentBeadIDKey: "ga-prev"})
	selfClaimReproCreate(t, rig, beads.Bead{ID: "ga-new", Title: "claimed via bd update --claim", Type: "task", Status: "in_progress", Assignee: "witness"})
	anchor, _ := rig.Get("ga-new")

	reconcileFreshCycleSelfClaimRepro(env, []beads.Bead{session}, []beads.Bead{anchor}, map[string]beads.Store{"gascity": rig})

	assertSelfClaimNotCycled(t, env, session, sessionName, "the session claimed ga-new itself (bd update --claim); no current_claim_bead_id is ever written on that path")
}

// The builder's live shape: current_claim_bead_id is STALE (an unrelated
// unassigned bead) because a raw bd release does not clear it, so it never
// equals any anchor. Q1's existence check does not depend on the stamp at
// all, so a stale stamp cannot hide the session's real, live claim.
func TestFreshCycleRepro_StaleSelfClaimStampDoesNotHideRealClaim(t *testing.T) {
	env, session, sessionName := freshCycleSelfClaimReproEnv(t, map[string]string{
		"awake_started_at":                     time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano),
		beadmeta.CurrentClaimBeadIDMetadataKey: "ga-stale",
	})
	rig := selfClaimReproMemStore()
	selfClaimReproCreate(t, rig, beads.Bead{ID: "ga-stale", Title: "released long ago", Type: "task", Status: "open"})
	selfClaimReproCreate(t, rig, beads.Bead{ID: "ga-prev", Title: "previous step", Type: "task", Status: "in_progress", Assignee: "witness"})
	selfClaimReproCloseForReal(t, rig, "ga-prev")
	env.setSessionMetadata(&session, map[string]string{sessionpkg.CurrentBeadIDKey: "ga-prev"})
	selfClaimReproCreate(t, rig, beads.Bead{ID: "ga-new", Title: "claimed step", Type: "task", Status: "in_progress", Assignee: "witness"})
	anchor, _ := rig.Get("ga-new")

	reconcileFreshCycleSelfClaimRepro(env, []beads.Bead{session}, []beads.Bead{anchor}, map[string]beads.Store{"gascity": rig})

	assertSelfClaimNotCycled(t, env, session, sessionName, "current_claim_bead_id is a stale unrelated bead; the session's real claim is ga-new")
}

// Mayor's ask (a): the session self-claimed the ROOT (source bead) while the
// reconciler's anchor is one of its STEPS. Q1's existence check is
// anchor-agnostic — it finds ga-src itself still in_progress+assigned in the
// same candidate population, regardless of what the anchor separately
// resolved to, so no molecule-membership comparison is needed.
func TestFreshCycleRepro_SelfClaimOfRootWhileAnchorIsStep(t *testing.T) {
	env, session, sessionName := freshCycleSelfClaimReproEnv(t, map[string]string{
		"awake_started_at":                     time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano),
		beadmeta.CurrentClaimBeadIDMetadataKey: "ga-src",
	})
	rig := selfClaimReproMemStore()
	selfClaimReproCreate(t, rig, beads.Bead{ID: "ga-mol", Title: "molecule", Type: "molecule", Status: "open"})
	selfClaimReproCreate(t, rig, beads.Bead{ID: "ga-src", Title: "source bead", Type: "task", Status: "in_progress", Assignee: "witness", Metadata: map[string]string{"molecule_id": "ga-mol"}})
	selfClaimReproCreate(t, rig, beads.Bead{ID: "ga-step1", Title: "RED", Type: "step", Status: "in_progress", Assignee: "witness", ParentID: "ga-mol"})
	selfClaimReproCloseForReal(t, rig, "ga-step1")
	selfClaimReproCreate(t, rig, beads.Bead{ID: "ga-step2", Title: "GREEN", Type: "step", Status: "in_progress", Assignee: "witness", ParentID: "ga-mol"})
	env.setSessionMetadata(&session, map[string]string{sessionpkg.CurrentBeadIDKey: "ga-step1"})
	step2, _ := rig.Get("ga-step2")

	reconcileFreshCycleSelfClaimRepro(env, []beads.Bead{session}, []beads.Bead{step2}, map[string]beads.Store{"gascity": rig})

	assertSelfClaimNotCycled(t, env, session, sessionName, "anchor ga-step2 is a step of the molecule whose source bead ga-src the session claimed")
}

// Mayor's ask (b), and live case 4 (23:05:15, ga-851h6i -> ga-g4odhq): the
// session claimed the next STEP while the anchor falls back to the
// molecule's own ROOT/source bead. Symmetric to the root/step case above.
func TestFreshCycleRepro_SelfClaimOfStepWhileAnchorIsRoot(t *testing.T) {
	env, session, sessionName := freshCycleSelfClaimReproEnv(t, map[string]string{
		"awake_started_at":                     time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano),
		beadmeta.CurrentClaimBeadIDMetadataKey: "ga-step2",
	})
	rig := selfClaimReproMemStore()
	selfClaimReproCreate(t, rig, beads.Bead{ID: "ga-mol", Title: "molecule", Type: "molecule", Status: "open"})
	selfClaimReproCreate(t, rig, beads.Bead{ID: "ga-src", Title: "source bead", Type: "task", Status: "in_progress", Assignee: "witness", Metadata: map[string]string{"molecule_id": "ga-mol"}})
	selfClaimReproCreate(t, rig, beads.Bead{ID: "ga-step1", Title: "RED", Type: "step", Status: "in_progress", Assignee: "witness", ParentID: "ga-mol"})
	selfClaimReproCloseForReal(t, rig, "ga-step1")
	selfClaimReproCreate(t, rig, beads.Bead{ID: "ga-step2", Title: "GREEN", Type: "step", Status: "in_progress", Assignee: "witness", ParentID: "ga-mol"})
	env.setSessionMetadata(&session, map[string]string{sessionpkg.CurrentBeadIDKey: "ga-step1"})
	src, _ := rig.Get("ga-src")
	step2, _ := rig.Get("ga-step2")

	// Anchor falls back to the FIRST matching work bead: the source bead.
	reconcileFreshCycleSelfClaimRepro(env, []beads.Bead{session}, []beads.Bead{src, step2}, map[string]beads.Store{"gascity": rig})

	assertSelfClaimNotCycled(t, env, session, sessionName, "anchor ga-src is the source bead of the molecule whose step ga-step2 the session claimed")
}

// Case 5 (23:18:04, gascity--investigator ga-knges1 -> ga-1sss9q): the tick
// decided on a ~4-minute-old assigned-work snapshot. The anchor it cycled the
// session ONTO was already closed by kill time and the session had already
// self-claimed its next bead. Q2's live re-read of the anchor sees it closed
// and skips the cycle.
func TestFreshCycleRepro_AnchorAlreadyClosedDoesNotCycle(t *testing.T) {
	env, session, sessionName := freshCycleSelfClaimReproEnv(t, map[string]string{
		"awake_started_at": time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano),
	})
	rig := selfClaimReproMemStore()
	selfClaimReproCreate(t, rig, beads.Bead{ID: "ga-prev", Title: "previous", Type: "task", Status: "in_progress", Assignee: "witness"})
	selfClaimReproCloseForReal(t, rig, "ga-prev")
	selfClaimReproCreate(t, rig, beads.Bead{ID: "ga-anchor", Title: "already finished", Type: "task", Status: "in_progress", Assignee: "witness"})
	staleSnapshot, _ := rig.Get("ga-anchor")        // what the slow tick still holds
	selfClaimReproCloseForReal(t, rig, "ga-anchor") // live truth at kill time
	env.setSessionMetadata(&session, map[string]string{sessionpkg.CurrentBeadIDKey: "ga-prev"})

	reconcileFreshCycleSelfClaimRepro(env, []beads.Bead{session}, []beads.Bead{staleSnapshot}, map[string]beads.Store{"gascity": rig})

	assertSelfClaimNotCycled(t, env, session, sessionName, "the anchor ga-anchor is already closed on a live read; cycling onto it is pure loss")
}
