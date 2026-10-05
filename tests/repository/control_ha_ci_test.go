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
	Runner  string           `json:"runs-on"`
	Timeout int              `json:"timeout-minutes"`
	Needs   []string         `json:"needs"`
	Steps   []haWorkflowStep `json:"steps"`
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
	job := jobs["control-ha"]
	if job.Runner != "ubuntu-24.04-arm" || !slices.Equal(job.Needs, []string{"container-arm64"}) || job.Timeout < 60 {
		t.Fatal("full control-HA gate must reuse the native arm64 image in a separately bounded job")
	}
	load := requireHAStep(t, job, "Verify and load exact-source HA image")
	for _, required := range []string{"sha256sum --check --strict", "docker load", "aarch64", "scripts/inspect-oci-image.sh", "epoch/node:ci-arm64", "${GITHUB_SHA}"} {
		if !strings.Contains(load.Run, required) {
			t.Errorf("image verification omitted %q", required)
		}
	}
	run := requireHAStep(t, job, "Prove the complete three/five-controller failure matrix")
	if run.If != "" || !strings.Contains(run.Run, "make test-control-ha-full") || !strings.Contains(run.Run, "tests/integration/control_ha_full.py verify") || run.Env["EPOCH_REGIONAL_IMAGE"] != "epoch/node:ci-arm64" || run.Env["EPOCH_REGIONAL_USE_EXISTING_IMAGE"] != "1" {
		t.Error("the required HA job must run and independently verify the complete live matrix")
	}
	upload := requireHAStep(t, job, "Upload complete control-HA evidence")
	if upload.If != "always()" || !strings.HasPrefix(upload.Uses, "actions/upload-artifact@") || upload.With["retention-days"] != float64(30) || upload.With["path"] != "${{ runner.temp }}/epoch-control-ha-full" {
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
