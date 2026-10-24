package repository_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

type haWorkflowStep struct {
	Name string            `json:"name"`
	If   string            `json:"if"`
	Uses string            `json:"uses"`
	Run  string            `json:"run"`
	Env  map[string]string `json:"env"`
	With map[string]any    `json:"with"`
}

type haWorkflowJob struct {
	Name     string           `json:"name"`
	If       string           `json:"if"`
	Runner   string           `json:"runs-on"`
	Timeout  int              `json:"timeout-minutes"`
	Needs    []string         `json:"needs"`
	Steps    []haWorkflowStep `json:"steps"`
	Strategy struct {
		FailFast    *bool `json:"fail-fast"`
		MaxParallel int   `json:"max-parallel"`
		Matrix      struct {
			Controllers []int `json:"controllers"`
		} `json:"matrix"`
	} `json:"strategy"`
}

func readHAWorkflow(t *testing.T) map[string]haWorkflowJob {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]haWorkflowJob `json:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	return workflow.Jobs
}

func requireHAStep(t *testing.T, job haWorkflowJob, name string) haWorkflowStep {
	t.Helper()
	for _, step := range job.Steps {
		if step.Name == name {
			return step
		}
	}
	t.Fatalf("missing HA workflow step %q", name)
	return haWorkflowStep{}
}

func TestControlHACIRunsTheFullMatrixOnTheReusedNativeImage(t *testing.T) {
	jobs := readHAWorkflow(t)
	job := jobs["control-ha-fleet"]
	if job.Runner != "ubuntu-24.04-arm" || !slices.Equal(job.Needs, []string{"container-arm64"}) || job.Timeout < 90 || job.Timeout > 120 {
		t.Fatal("full control-HA gate must reuse the native arm64 image in a separately bounded job")
	}
	if job.Strategy.FailFast == nil || *job.Strategy.FailFast || job.Strategy.MaxParallel != 2 || !slices.Equal(job.Strategy.Matrix.Controllers, []int{3, 5}) {
		t.Fatal("both complete fleets must run on separate parallel runners without cancelling the other proof")
	}
	load := requireHAStep(t, job, "Verify and load exact-source HA image")
	for _, required := range []string{"sha256sum --check --strict", "docker load", "aarch64", "scripts/inspect-oci-image.sh", "epoch/node:ci-arm64", "${GITHUB_SHA}"} {
		if !strings.Contains(load.Run, required) {
			t.Errorf("image verification omitted %q", required)
		}
	}
	run := requireHAStep(t, job, "Prove and independently verify this complete fleet")
	if run.If != "" || !strings.Contains(run.Run, "management_sdk_catalog.py run") || !strings.Contains(run.Run, "management_sdk_catalog.py verify") || run.Env["EPOCH_CONTROL_HA_CONTROLLERS"] != "${{ matrix.controllers }}" || run.Env["EPOCH_REGIONAL_IMAGE"] != "epoch/node:ci-arm64" || run.Env["EPOCH_REGIONAL_USE_EXISTING_IMAGE"] != "1" || run.Env["PYTHONPATH"] != "sdk/python/src" {
		t.Error("each worker must run and independently verify its complete live fault matrix")
	}
	upload := requireHAStep(t, job, "Upload this fleet's passing or failed evidence")
	if upload.If != "always()" || !strings.HasPrefix(upload.Uses, "actions/upload-artifact@") || upload.With["retention-days"] != float64(30) || upload.With["name"] != "control-ha-fleet-${{ matrix.controllers }}-${{ github.run_attempt }}" || upload.With["path"] != "${{ runner.temp }}/epoch-control-ha-fleet" {
		t.Error("failed or passing HA attempts must retain their complete evidence for 30 days")
	}

	producer := jobs["container-arm64"]
	save := requireHAStep(t, producer, "Export the exact-source image for HA testing")
	if !strings.Contains(save.Run, "docker save") || !strings.Contains(save.Run, "epoch/node:ci-arm64") || !strings.Contains(save.Run, "sha256sum node.tar") {
		t.Error("the producer must export its already inspected image with an archive checksum")
	}
	imageUpload := requireHAStep(t, producer, "Share the native image with the HA gate")
	download := requireHAStep(t, job, "Download the exact-source native image")
	if imageUpload.With["name"] != download.With["name"] || imageUpload.With["name"] != "control-ha-node-arm64-${{ github.run_attempt }}" || !strings.HasPrefix(download.Uses, "actions/download-artifact@") {
		t.Error("the HA gate must consume this attempt's exact native image artifact")
	}
	for _, step := range job.Steps {
		if strings.Contains(step.Run, "docker build") || strings.Contains(step.Run, "docker push") {
			t.Error("the HA gate must not rebuild or publish the tested node image")
		}
	}
}

func TestControlHAProtectedGateRequiresBothCurrentAttemptFleetProofs(t *testing.T) {
	job := readHAWorkflow(t)["control-ha"]
	if job.Name != "Concurrent control-plane failure matrix" || job.If != "always()" || !slices.Equal(job.Needs, []string{"control-ha-fleet"}) || job.Timeout < 5 || job.Timeout > 15 {
		t.Fatal("aggregate protected check must run even when a fleet failed or was skipped")
	}
	result := requireHAStep(t, job, "Require both fleet workers to have succeeded")
	if result.If != "" || result.Env["FLEET_RESULT"] != "${{ needs.control-ha-fleet.result }}" || !strings.Contains(result.Run, `test "${FLEET_RESULT}" = success`) {
		t.Error("failed, cancelled, missing, or skipped workers must fail the protected gate")
	}
	for _, count := range []string{"3", "5"} {
		download := requireHAStep(t, job, "Download verified "+count+"-controller proof")
		if download.With["name"] != "control-ha-fleet-"+count+"-${{ github.run_attempt }}" || download.With["path"] != "${{ runner.temp }}/epoch-control-ha-input/"+count || !strings.HasPrefix(download.Uses, "actions/download-artifact@") {
			t.Error("aggregate must consume both exact current-attempt fleet artifacts")
		}
	}
	seal := requireHAStep(t, job, "Seal and independently verify the complete failure matrix")
	for _, required := range []string{"management_sdk_catalog.py combine", "--three", "--five", "management_sdk_catalog.py verify", "epoch-control-ha-full/evidence.json"} {
		if !strings.Contains(seal.Run, required) {
			t.Errorf("aggregate seal omitted %q", required)
		}
	}
	if seal.If != "" {
		t.Error("full verification must not be conditional")
	}
	upload := requireHAStep(t, job, "Upload complete control-HA evidence")
	if upload.If != "always()" || upload.With["retention-days"] != float64(30) || upload.With["if-no-files-found"] != "error" {
		t.Error("complete proof must be retained and missing evidence must fail closed")
	}
}

func TestControlHACertifiesAllPublicSDKsWithoutReplacingNativeFaults(t *testing.T) {
	jobs := readHAWorkflow(t)
	fleet := jobs["control-ha-fleet"]
	python, java := false, false
	for _, step := range fleet.Steps {
		if strings.HasPrefix(step.Uses, "actions/setup-python@") && step.With["python-version"] == "3.11" && step.If == "" {
			python = true
		}
		if strings.HasPrefix(step.Uses, "actions/setup-java@") && step.With["java-version"] == "25" && step.If == "" {
			java = true
		}
	}
	if !python || !java {
		t.Error("each live fleet must require Python and Java as well as Go, without optional runtime skips")
	}
	install := requireHAStep(t, fleet, "Install public SDK Catalog campaign dependencies")
	if install.If != "" || !strings.Contains(install.Run, "sdk/python[management]") || !strings.Contains(install.Run, "test_management_sdk_catalog") {
		t.Error("public SDK runtime and evidence contracts must be installed/executed before faults")
	}
	aggregate := requireHAStep(t, jobs["control-ha"], "Install independent public SDK evidence verifier")
	if aggregate.If != "" || !strings.Contains(aggregate.Run, "sdk/python[management]") {
		t.Error("complete aggregate must independently parse SDK protobuf witnesses")
	}
	for _, marker := range []struct{ path, required string }{
		{"management_sdk_catalog.py", "class SDKFleet(full.FullFleet)"},
		{"management_sdk_catalog.py", "bundle_verifier=lambda manifest: verify_bundle(manifest, counts)"},
		{"management_sdk_catalog_evidence.py", "full.verify_full_bundle("},
		{"management_sdk_catalog_evidence.py", "full.validate_full_evidence("},
	} {
		data, err := os.ReadFile(filepath.Join("..", "integration", marker.path))
		if err != nil || !strings.Contains(string(data), marker.required) {
			t.Errorf("SDK gate must compose rather than replace the native full fault gate: %s", marker.path)
		}
	}
}
