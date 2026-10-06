package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const externalRigBeadsConfig = `issue_prefix: rig
gc.endpoint_origin: explicit
gc.endpoint_status: verified
dolt.auto-start: false
dolt.host: central-dolt.example.com
dolt.port: 3306
dolt.user: orchestrator
`

func writeExternalRigScope(t *testing.T, cityPath string) string {
	t.Helper()
	rigDir := filepath.Join(cityPath, "rigs", "ext")
	if err := os.MkdirAll(filepath.Join(rigDir, ".beads"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rigDir, ".beads", "config.yaml"), []byte(externalRigBeadsConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	return rigDir
}

// B1: allocate-port must honor GC_DOLT_PORT for an external endpoint even
// while a live managed Dolt state exists for the city.
func TestChooseManagedDoltPortExternalEnvBeatsLiveManagedState(t *testing.T) {
	tests := []struct {
		name        string
		host        string
		wantManaged bool
	}{
		{name: "explicit external host", host: "central-dolt.example.com", wantManaged: false},
		{name: "stale env port, no host", host: "", wantManaged: true},
		{name: "loopback host is still managed", host: "127.0.0.1", wantManaged: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cityPath := t.TempDir()
			clearManagedDoltRuntimeEnvForTest(t)
			managedPort := writeReachableProviderManagedDoltState(t, cityPath)
			t.Setenv("GC_DOLT_HOST", tc.host)
			t.Setenv("GC_DOLT_PORT", "4406")

			got, err := chooseManagedDoltPort(cityPath, "")
			if err != nil {
				t.Fatalf("chooseManagedDoltPort: %v", err)
			}
			want := "4406"
			if tc.wantManaged {
				want = strconv.Itoa(managedPort)
			}
			if got != want {
				t.Fatalf("chooseManagedDoltPort = %q, want %q (managed port %d)", got, want, managedPort)
			}
		})
	}
}

// B2: a rig bound to an external Dolt must not be verified against the city's
// managed-local catalog.
func TestVerifyManagedDoltDatabaseSkipsExternalRigUnderManagedCity(t *testing.T) {
	tests := []struct {
		name         string
		externalRig  bool
		wantListerOn bool
	}{
		{name: "external rig skips managed catalog", externalRig: true, wantListerOn: false},
		{name: "managed rig still checks managed catalog", externalRig: false, wantListerOn: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cityPath := t.TempDir()
			clearManagedDoltRuntimeEnvForTest(t)
			writeReachableProviderManagedDoltState(t, cityPath)
			rigDir := filepath.Join(cityPath, "rigs", "managed")
			if tc.externalRig {
				rigDir = writeExternalRigScope(t, cityPath)
			} else if err := os.MkdirAll(rigDir, 0o755); err != nil {
				t.Fatal(err)
			}
			if isExternalDolt(cityPath) {
				t.Fatal("precondition: city must be managed-local")
			}

			orig := managedDoltListUserDatabasesAfterInit
			t.Cleanup(func() { managedDoltListUserDatabasesAfterInit = orig })
			called := false
			managedDoltListUserDatabasesAfterInit = func(string) ([]string, error) {
				called = true
				return []string{"hq"}, nil
			}

			err := verifyManagedDoltDatabaseExistsAfterInit(cityPath, rigDir, "bd_rig_db")
			if called != tc.wantListerOn {
				t.Fatalf("managed catalog listed = %v, want %v", called, tc.wantListerOn)
			}
			if tc.wantListerOn {
				if err == nil || !strings.Contains(err.Error(), "not found in managed Dolt server catalog") {
					t.Fatalf("err = %v, want managed-catalog not-found", err)
				}
			} else if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
		})
	}
}

// P19/P20 shape: managed city with a live local Dolt plus an external rig.
// Both the port allocation and the post-init catalog check must leave the
// external endpoint alone, or `gc start` dials the wrong port and the
// controller crashloops.
func TestExternalRigUnderLiveManagedCityCombined(t *testing.T) {
	cityPath := t.TempDir()
	clearManagedDoltRuntimeEnvForTest(t)
	managedPort := writeReachableProviderManagedDoltState(t, cityPath)
	rigDir := writeExternalRigScope(t, cityPath)
	t.Setenv("GC_DOLT_HOST", "central-dolt.example.com")
	t.Setenv("GC_DOLT_PORT", "3306")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"dolt-state", "allocate-port", "--city", cityPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("allocate-port exit %d: %s", code, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "3306" {
		t.Fatalf("allocate-port = %q, want external 3306 (managed port is %d)", got, managedPort)
	}

	orig := managedDoltListUserDatabasesAfterInit
	t.Cleanup(func() { managedDoltListUserDatabasesAfterInit = orig })
	managedDoltListUserDatabasesAfterInit = func(string) ([]string, error) {
		return nil, fmt.Errorf("managed catalog must not be consulted for an external rig")
	}
	if err := verifyManagedDoltDatabaseExistsAfterInit(cityPath, rigDir, "bd_rig_db"); err != nil {
		t.Fatalf("verify for external rig = %v, want nil", err)
	}
}
