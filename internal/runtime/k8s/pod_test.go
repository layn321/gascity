package k8s

import (
	"encoding/base64"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/shellquote"
)

func TestBuildPod_NodeSelector(t *testing.T) {
	p := newProviderWithOps(newFakeK8sOps())
	p.nodeSelector = map[string]string{"workload": "gc-agents"}
	pod, err := buildPod("test-session", runtime.Config{Command: "/bin/bash"}, p)
	if err != nil {
		t.Fatalf("buildPod: %v", err)
	}
	if pod.Spec.NodeSelector["workload"] != "gc-agents" {
		t.Errorf("NodeSelector[workload] = %q, want \"gc-agents\"", pod.Spec.NodeSelector["workload"])
	}
}

func TestBuildPod_Tolerations(t *testing.T) {
	p := newProviderWithOps(newFakeK8sOps())
	p.tolerations = []corev1.Toleration{{
		Key: "gc-agents", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule,
	}}
	pod, err := buildPod("test-session", runtime.Config{Command: "/bin/bash"}, p)
	if err != nil {
		t.Fatalf("buildPod: %v", err)
	}
	if len(pod.Spec.Tolerations) != 1 {
		t.Fatalf("len(Tolerations) = %d, want 1", len(pod.Spec.Tolerations))
	}
	if pod.Spec.Tolerations[0].Key != "gc-agents" {
		t.Errorf("Toleration.Key = %q, want \"gc-agents\"", pod.Spec.Tolerations[0].Key)
	}
}

func TestBuildPod_Affinity(t *testing.T) {
	p := newProviderWithOps(newFakeK8sOps())
	p.affinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key: "node-type", Operator: corev1.NodeSelectorOpIn, Values: []string{"gpu"},
					}},
				}},
			},
		},
	}
	pod, err := buildPod("test-session", runtime.Config{Command: "/bin/bash"}, p)
	if err != nil {
		t.Fatalf("buildPod: %v", err)
	}
	if pod.Spec.Affinity == nil {
		t.Fatal("Affinity is nil")
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		t.Fatal("NodeAffinity is nil")
	}
	expressions := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions
	if expressions[0].Values[0] != "gpu" {
		t.Fatalf("affinity value = %q, want gpu", expressions[0].Values[0])
	}
}

func TestBuildPod_PriorityClassName(t *testing.T) {
	p := newProviderWithOps(newFakeK8sOps())
	p.priorityClassName = "gc-agent-high"
	pod, err := buildPod("test-session", runtime.Config{Command: "/bin/bash"}, p)
	if err != nil {
		t.Fatalf("buildPod: %v", err)
	}
	if pod.Spec.PriorityClassName != "gc-agent-high" {
		t.Errorf("PriorityClassName = %q, want \"gc-agent-high\"", pod.Spec.PriorityClassName)
	}
}

func TestBuildPod_NoSchedulingFields_NoBehaviorChange(t *testing.T) {
	// Zero-value scheduling fields must not alter default pod behavior.
	p := newProviderWithOps(newFakeK8sOps())
	pod, err := buildPod("test-session", runtime.Config{Command: "/bin/bash"}, p)
	if err != nil {
		t.Fatalf("buildPod: %v", err)
	}
	if pod.Spec.NodeSelector != nil {
		t.Errorf("NodeSelector should be nil when not set")
	}
	if len(pod.Spec.Tolerations) != 0 {
		t.Errorf("Tolerations should be empty when not set")
	}
	if pod.Spec.Affinity != nil {
		t.Errorf("Affinity should be nil when not set")
	}
	if pod.Spec.PriorityClassName != "" {
		t.Errorf("PriorityClassName should be empty when not set")
	}
}

func TestBuildPod_ClonesSchedulingFields(t *testing.T) {
	seconds := int64(30)
	p := newProviderWithOps(newFakeK8sOps())
	p.nodeSelector = map[string]string{"workload": "gc-agents"}
	p.tolerations = []corev1.Toleration{{
		Key:               "gc-agents",
		Operator:          corev1.TolerationOpExists,
		Effect:            corev1.TaintEffectNoSchedule,
		TolerationSeconds: &seconds,
	}}
	p.affinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key: "node-type", Operator: corev1.NodeSelectorOpIn, Values: []string{"gpu"},
					}},
				}},
			},
		},
	}

	pod, err := buildPod("test-session", runtime.Config{Command: "/bin/bash"}, p)
	if err != nil {
		t.Fatalf("buildPod: %v", err)
	}

	pod.Spec.NodeSelector["workload"] = "changed"
	pod.Spec.Tolerations[0].Key = "changed"
	pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values[0] = "changed"

	if p.nodeSelector["workload"] != "gc-agents" {
		t.Fatalf("provider nodeSelector mutated to %q", p.nodeSelector["workload"])
	}
	if p.tolerations[0].Key != "gc-agents" {
		t.Fatalf("provider toleration key mutated to %q", p.tolerations[0].Key)
	}
	values := p.affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values
	if values[0] != "gpu" {
		t.Fatalf("provider affinity value mutated to %q", values[0])
	}
}

// perBeadWorkDirConfig is a pool/workflow worker's runtime config: WorkDir is a
// per-bead directory under the rig (<rig>/<beadID>-<slug>) that nothing has
// created yet.
func perBeadWorkDirConfig() runtime.Config {
	return runtime.Config{
		Command: "/bin/bash",
		WorkDir: "/city/rigs/testrig/tr-abc-slug",
		Env:     map[string]string{"GC_CITY": "/city"},
	}
}

const perBeadPodWorkDir = "/workspace/rigs/testrig/tr-abc-slug"

// TestBuildPod_WorkingDirIsAlwaysAnExistingPath pins that the pod spec never
// names a directory that may not exist yet. The kubelet chdirs into the
// container's WorkingDir before the entrypoint runs, so a per-bead WorkingDir
// is created by the runtime as root:root (containerd) or rejected outright —
// either way no command, including pre_start, gets to create it correctly.
// The workspace root always exists (EmptyDir mount when staged, WORKDIR in the
// prebaked image), so the spec points there and the entrypoint enters the
// per-bead directory itself.
func TestBuildPod_WorkingDirIsAlwaysAnExistingPath(t *testing.T) {
	for _, prebaked := range []bool{false, true} {
		name := "staged"
		if prebaked {
			name = "prebaked"
		}
		t.Run(name, func(t *testing.T) {
			p := newProviderWithOps(newFakeK8sOps())
			p.prebaked = prebaked
			pod, err := buildPod("test-session", perBeadWorkDirConfig(), p)
			if err != nil {
				t.Fatalf("buildPod: %v", err)
			}
			if got := pod.Spec.Containers[0].WorkingDir; got != podWorkspaceRoot {
				t.Errorf("WorkingDir = %q, want %q (a path guaranteed to exist)", got, podWorkspaceRoot)
			}
		})
	}
}

// TestBuildPod_EntrypointCreatesAndEntersWorkDir pins that the entrypoint
// creates the per-bead WorkingDir and cds into it, so the agent still starts in
// its own directory. This must hold for prebaked images too: prebaked pods
// mount no shared volume, so an init container physically cannot create a
// directory the main container would see.
func TestBuildPod_EntrypointCreatesAndEntersWorkDir(t *testing.T) {
	for _, prebaked := range []bool{false, true} {
		name := "staged"
		if prebaked {
			name = "prebaked"
		}
		t.Run(name, func(t *testing.T) {
			p := newProviderWithOps(newFakeK8sOps())
			p.prebaked = prebaked
			pod, err := buildPod("test-session", perBeadWorkDirConfig(), p)
			if err != nil {
				t.Fatalf("buildPod: %v", err)
			}
			args := strings.Join(pod.Spec.Containers[0].Args, " ")
			quoted := shellquote.Quote(perBeadPodWorkDir)
			if !strings.Contains(args, "mkdir -p "+quoted) {
				t.Errorf("entrypoint should mkdir the per-bead WorkingDir; got: %s", args)
			}
			if !strings.Contains(args, "cd "+quoted) {
				t.Errorf("entrypoint should cd into the per-bead WorkingDir; got: %s", args)
			}
		})
	}
}

// TestBuildPod_EntrypointCreatesWorkDirAsDynamicUser pins the same contract on
// the LINUX_USERNAME path, where root creates and chowns the directory before
// dropping privileges and the tmux session cds into it.
func TestBuildPod_EntrypointCreatesWorkDirAsDynamicUser(t *testing.T) {
	p := newProviderWithOps(newFakeK8sOps())
	cfg := perBeadWorkDirConfig()
	cfg.Env["LINUX_USERNAME"] = "gcagent"

	pod, err := buildPod("test-session", cfg, p)
	if err != nil {
		t.Fatalf("buildPod: %v", err)
	}
	args := strings.Join(pod.Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "mkdir -p \""+perBeadPodWorkDir+"\"") {
		t.Errorf("entrypoint should mkdir the per-bead WorkingDir as root; got: %s", args)
	}
	if !strings.Contains(args, "cd "+perBeadPodWorkDir) {
		t.Errorf("tmux session should start in the per-bead WorkingDir; got: %s", args)
	}
}

// TestBuildPod_EntersWorkDirAfterStagingAndBeforePreStart pins the ordering of
// the entrypoint, which two silent regressions depend on. Entering the work dir
// must happen after the staging wait, or the shell sits in a subdirectory of a
// workspace that is still being written. And it must happen before pre_start,
// because pre_start used to run in the container's WorkingDir — which was the
// per-bead dir — and commands there may use relative paths.
func TestBuildPod_EntersWorkDirAfterStagingAndBeforePreStart(t *testing.T) {
	for _, username := range []string{"", "gcagent"} {
		name := "no-dynamic-user"
		if username != "" {
			name = "dynamic-user"
		}
		t.Run(name, func(t *testing.T) {
			p := newProviderWithOps(newFakeK8sOps())
			cfg := perBeadWorkDirConfig()
			cfg.PreStart = []string{"echo pre-start-marker"}
			if username != "" {
				cfg.Env["LINUX_USERNAME"] = username
			}
			pod, err := buildPod("test-session", cfg, p)
			if err != nil {
				t.Fatalf("buildPod: %v", err)
			}
			args := strings.Join(pod.Spec.Containers[0].Args, " ")

			stagingWait := strings.Index(args, ".gc-workspace-ready")
			enter := strings.Index(args, "cd "+shellquote.Quote(perBeadPodWorkDir))
			// pre_start commands are base64-encoded into the entrypoint.
			preStart := strings.Index(args, base64.StdEncoding.EncodeToString([]byte("echo pre-start-marker")))

			if stagingWait < 0 || enter < 0 || preStart < 0 {
				t.Fatalf("entrypoint missing a stage (wait=%d enter=%d preStart=%d): %s",
					stagingWait, enter, preStart, args)
			}
			if enter < stagingWait {
				t.Errorf("entering the work dir must come after the staging wait; got: %s", args)
			}
			if preStart < enter {
				t.Errorf("pre_start must run after entering the work dir; got: %s", args)
			}
		})
	}
}

// TestBuildPod_InitContainerOnlyWaitsForStaging pins that the staging init
// container is back to a single responsibility — waiting for the controller to
// finish staging. Creating the WorkingDir there only ever worked for staged,
// non-prebaked pods; the entrypoint now owns it for every topology.
func TestBuildPod_InitContainerOnlyWaitsForStaging(t *testing.T) {
	p := newProviderWithOps(newFakeK8sOps())
	pod, err := buildPod("test-session", perBeadWorkDirConfig(), p)
	if err != nil {
		t.Fatalf("buildPod: %v", err)
	}
	if len(pod.Spec.InitContainers) != 1 {
		t.Fatalf("len(InitContainers) = %d, want 1", len(pod.Spec.InitContainers))
	}
	cmd := strings.Join(pod.Spec.InitContainers[0].Command, " ")
	if strings.Contains(cmd, "mkdir") {
		t.Errorf("init container should not create the WorkingDir; got: %s", cmd)
	}
	if !strings.Contains(cmd, ".gc-ready") {
		t.Errorf("init container should wait for staging; got: %s", cmd)
	}
}

// f5Sentinel is a fake password used only to prove it never reaches the spec.
const f5Sentinel = "f5-sentinel-not-a-real-credential"

func f5Config() runtime.Config {
	return runtime.Config{
		Command: "/bin/bash",
		Env: map[string]string{
			"GC_DOLT_HOST":           "dolt.example",
			"GC_DOLT_PORT":           "3307",
			"GC_DOLT_USER":           "agent",
			"GC_DOLT_PASSWORD":       f5Sentinel,
			"BEADS_DOLT_PASSWORD":    f5Sentinel,
			"BEADS_CREDENTIALS_FILE": "/controller/side/credentials",
		},
	}
}

func podHasLiteral(pod *corev1.Pod, lit string) bool {
	for _, c := range append(append([]corev1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...) {
		for _, e := range c.Env {
			if strings.Contains(e.Value, lit) {
				return true
			}
		}
		for _, a := range append(append([]string{}, c.Command...), c.Args...) {
			if strings.Contains(a, lit) {
				return true
			}
		}
	}
	return false
}

func TestBuildPod_DoltPasswordNeverLiteral(t *testing.T) {
	for _, configured := range []bool{false, true} {
		p := newProviderWithOps(newFakeK8sOps())
		if configured {
			p.doltCredsSecret, p.doltCredsKey = "beads-dolt-creds-file", "credentials"
		}
		pod, err := buildPod("test-session", f5Config(), p)
		if err != nil {
			t.Fatalf("buildPod: %v", err)
		}
		if podHasLiteral(pod, f5Sentinel) {
			t.Errorf("configured=%v: password literal present in pod spec", configured)
		}
		for _, e := range pod.Spec.Containers[0].Env {
			if e.Name == "GC_DOLT_PASSWORD" || e.Name == "BEADS_DOLT_PASSWORD" {
				t.Errorf("configured=%v: env %s must not be set on the pod", configured, e.Name)
			}
		}
	}
}

func TestBuildPod_DoltCredentialsFileWhenConfigured(t *testing.T) {
	p := newProviderWithOps(newFakeK8sOps())
	p.doltCredsSecret, p.doltCredsKey = "beads-dolt-creds-file", "creds"
	pod, err := buildPod("test-session", f5Config(), p)
	if err != nil {
		t.Fatalf("buildPod: %v", err)
	}
	var vol *corev1.Volume
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Name == doltCredsVolume {
			vol = &pod.Spec.Volumes[i]
		}
	}
	if vol == nil || vol.Secret == nil || vol.Secret.SecretName != "beads-dolt-creds-file" {
		t.Fatalf("dolt credentials Secret volume missing or wrong: %+v", vol)
	}
	if len(vol.Secret.Items) != 1 || vol.Secret.Items[0].Key != "creds" || vol.Secret.Items[0].Path != doltCredsFileName {
		t.Errorf("Secret items = %+v", vol.Secret.Items)
	}
	if vol.Secret.Optional != nil && *vol.Secret.Optional {
		t.Error("configured Secret must be required, not optional")
	}
	var mounted bool
	for _, m := range pod.Spec.Containers[0].VolumeMounts {
		if m.Name == doltCredsVolume {
			mounted = m.ReadOnly && m.MountPath == doltCredsMountDir
		}
	}
	if !mounted {
		t.Error("credentials volume not mounted read-only at expected dir")
	}
	var vals []string
	for _, e := range pod.Spec.Containers[0].Env {
		if e.Name == "BEADS_CREDENTIALS_FILE" {
			vals = append(vals, e.Value)
		}
	}
	want := doltCredsMountDir + "/" + doltCredsFileName
	if len(vals) != 1 || vals[0] != want {
		t.Errorf("BEADS_CREDENTIALS_FILE = %v, want exactly [%s] (controller value replaced)", vals, want)
	}
}

func TestBuildPod_DoltCredentialsUnconfiguredUnchanged(t *testing.T) {
	p := newProviderWithOps(newFakeK8sOps())
	pod, err := buildPod("test-session", f5Config(), p)
	if err != nil {
		t.Fatalf("buildPod: %v", err)
	}
	for _, v := range pod.Spec.Volumes {
		if v.Name == doltCredsVolume {
			t.Error("credentials volume present while unconfigured")
		}
	}
	for _, m := range pod.Spec.Containers[0].VolumeMounts {
		if m.Name == doltCredsVolume {
			t.Error("credentials mount present while unconfigured")
		}
	}
	// Pass-through of unrelated env is unchanged, including the controller's own value.
	for _, e := range pod.Spec.Containers[0].Env {
		if e.Name == "BEADS_CREDENTIALS_FILE" && e.Value != "/controller/side/credentials" {
			t.Errorf("BEADS_CREDENTIALS_FILE rewritten while unconfigured: %q", e.Value)
		}
	}
}
