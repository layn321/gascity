package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// writeLiveManagedStateForExternalTest plants a valid, live managed-local Dolt
// provider state for cityPath and returns its port plus the state file path.
func writeLiveManagedStateForExternalTest(t *testing.T, cityPath string) (int, string) {
	t.Helper()
	stateFile := filepath.Join(t.TempDir(), "dolt-provider-state.json")
	layout, err := resolveManagedDoltRuntimeLayout(cityPath)
	if err != nil {
		t.Fatalf("resolveManagedDoltRuntimeLayout: %v", err)
	}
	listener := listenOnRandomPort(t)
	t.Cleanup(func() { _ = listener.Close() })
	port := listener.Addr().(*net.TCPAddr).Port
	if err := writeDoltRuntimeStateFile(stateFile, doltRuntimeState{
		Running:   true,
		PID:       os.Getpid(),
		Port:      port,
		DataDir:   layout.DataDir,
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("writeDoltRuntimeStateFile: %v", err)
	}
	return port, stateFile
}

// writeExternalRigScopeForTest writes an explicit external-endpoint rig scope
// config under a new rig dir inside cityPath and returns the rig dir.
func writeExternalRigScopeForTest(t *testing.T, cityPath string) string {
	t.Helper()
	rigDir := filepath.Join(cityPath, "rigs", "cdp")
	if err := os.MkdirAll(filepath.Join(rigDir, ".beads"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := `issue_prefix: cdp
gc.endpoint_origin: explicit
gc.endpoint_status: verified
dolt.auto-start: false
dolt.host: central.dolt.example.com
dolt.port: 3307
dolt.user: orchestrator
`
	if err := os.WriteFile(filepath.Join(rigDir, ".beads", "config.yaml"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	return rigDir
}

func setExternalDoltEnvForTest(t *testing.T) {
	t.Helper()
	t.Setenv("GC_DOLT_HOST", "central.dolt.example.com")
	t.Setenv("GC_DOLT_PORT", "3307")
	t.Setenv("BEADS_DOLT_SERVER_SOCKET", "")
}

// B1: with a live managed state present, an explicit external GC_DOLT_HOST
// plus GC_DOLT_PORT must win over the managed-local port.
func TestChooseManagedDoltPortExternalEnvBeatsManagedState(t *testing.T) {
	cityPath := t.TempDir()
	clearManagedDoltRuntimeEnvForTest(t)
	port, stateFile := writeLiveManagedStateForExternalTest(t, cityPath)
	setExternalDoltEnvForTest(t)

	got, err := chooseManagedDoltPort(cityPath, stateFile)
	if err != nil {
		t.Fatalf("chooseManagedDoltPort: %v", err)
	}
	if got != "3307" {
		t.Fatalf("chooseManagedDoltPort = %q, want external GC_DOLT_PORT 3307 (managed state port was %d)", got, port)
	}
}

// B1 control: a stale ambient port with a local host must still lose to live
// managed state (existing behavior, pinned so the fix cannot widen).
func TestChooseManagedDoltPortLocalHostEnvStillLosesToManagedState(t *testing.T) {
	cityPath := t.TempDir()
	clearManagedDoltRuntimeEnvForTest(t)
	port, stateFile := writeLiveManagedStateForExternalTest(t, cityPath)
	t.Setenv("GC_DOLT_HOST", "127.0.0.1")
	t.Setenv("GC_DOLT_PORT", "3307")

	got, err := chooseManagedDoltPort(cityPath, stateFile)
	if err != nil {
		t.Fatalf("chooseManagedDoltPort: %v", err)
	}
	if got != strconv.Itoa(port) {
		t.Fatalf("chooseManagedDoltPort = %q, want managed state port %d", got, port)
	}
}

// B2: a rig with its own external endpoint skips the managed-local catalog
// check even though the city is managed-local with a resolvable port.
func TestVerifyManagedDoltDatabaseSkipsExternalRigInManagedCity(t *testing.T) {
	cityPath := setupBdContractCityForTest(t)
	writeReachableProviderManagedDoltState(t, cityPath)
	rigDir := writeExternalRigScopeForTest(t, cityPath)

	if isExternalDolt(cityPath) {
		t.Fatalf("precondition: city must be managed-local")
	}
	if currentResolvableManagedDoltPort(cityPath) == "" {
		t.Fatalf("precondition: need a resolvable managed port so the guard is observable")
	}

	orig := managedDoltListUserDatabasesAfterInit
	t.Cleanup(func() { managedDoltListUserDatabasesAfterInit = orig })
	called := false
	managedDoltListUserDatabasesAfterInit = func(string) ([]string, error) {
		called = true
		return []string{"hq"}, nil
	}

	if err := verifyManagedDoltDatabaseExistsAfterInit(cityPath, rigDir, "cdp"); err != nil {
		t.Fatalf("verify for external rig = %v, want nil", err)
	}
	if called {
		t.Fatalf("managed catalog lister ran for an external rig scope")
	}
}

// B2 control: a managed-local rig scope still gets the catalog check.
func TestVerifyManagedDoltDatabaseStillChecksManagedRig(t *testing.T) {
	cityPath := setupBdContractCityForTest(t)
	writeReachableProviderManagedDoltState(t, cityPath)
	rigDir := filepath.Join(cityPath, "rigs", "local")
	if err := os.MkdirAll(filepath.Join(rigDir, ".beads"), 0o755); err != nil {
		t.Fatal(err)
	}

	orig := managedDoltListUserDatabasesAfterInit
	t.Cleanup(func() { managedDoltListUserDatabasesAfterInit = orig })
	called := false
	managedDoltListUserDatabasesAfterInit = func(string) ([]string, error) {
		called = true
		return []string{"hq"}, nil
	}

	err := verifyManagedDoltDatabaseExistsAfterInit(cityPath, rigDir, "local")
	if !called {
		t.Fatalf("managed catalog lister did not run for a managed-local rig")
	}
	if err == nil {
		t.Fatalf("verify = nil, want database-not-found error for a managed-local rig")
	}
}

// Combined P19/P20 crashloop shape: managed-local city with live managed state,
// ambient external endpoint, and a rig whose database lives on the central
// server. Port allocation must return the external port (P19) and the
// post-init catalog check must be skipped for that rig (P20).
func TestExternalRigInManagedCityCrashloopShape(t *testing.T) {
	cityPath := setupBdContractCityForTest(t)
	clearManagedDoltRuntimeEnvForTest(t)
	port, stateFile := writeLiveManagedStateForExternalTest(t, cityPath)
	writeReachableProviderManagedDoltState(t, cityPath)
	rigDir := writeExternalRigScopeForTest(t, cityPath)
	setExternalDoltEnvForTest(t)

	got, err := chooseManagedDoltPort(cityPath, stateFile)
	if err != nil {
		t.Fatalf("chooseManagedDoltPort: %v", err)
	}
	if got != "3307" {
		t.Fatalf("P19: allocate = %q, want 3307 (managed port %d)", got, port)
	}

	orig := managedDoltListUserDatabasesAfterInit
	t.Cleanup(func() { managedDoltListUserDatabasesAfterInit = orig })
	managedDoltListUserDatabasesAfterInit = func(string) ([]string, error) {
		return nil, fmt.Errorf("P20: managed catalog consulted for an external rig")
	}
	if err := verifyManagedDoltDatabaseExistsAfterInit(cityPath, rigDir, "cdp"); err != nil {
		t.Fatalf("P20: verify = %v, want nil", err)
	}
}
