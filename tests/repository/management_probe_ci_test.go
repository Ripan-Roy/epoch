package repository_test

import (
	"strings"
	"testing"
)

func TestManagementProbeMatrixIsRequiredRatherThanSilentlySkipped(t *testing.T) {
	job := readHAWorkflow(t)["integration"]
	step := requireHAStep(t, job, "Prove all public SDK probes retain real transport-loss outcomes")
	if step.If != "" || step.Env["EPOCH_MANAGEMENT_PROBE_MATRIX"] != "1" {
		t.Fatal("protected cross-language probe matrix must be mandatory and explicitly enabled")
	}
	for _, key := range []string{"EPOCH_MANAGEMENT_GO_PROBE", "EPOCH_MANAGEMENT_PYTHON_EXECUTABLE", "EPOCH_MANAGEMENT_JAVA_EXECUTABLE"} {
		if step.Env[key] == "" {
			t.Errorf("required public probe runtime %s omitted", key)
		}
	}
	for _, required := range []string{"sdk/python[management,management-dev]", "ManagementCatalogProbe", "managementsdkgo", "dependency:build-classpath", "go test -race", "TestPublicProbeMatrix", "-count=1", "EPOCH_MANAGEMENT_JAVA_CLASSPATH"} {
		if !strings.Contains(step.Run, required) {
			t.Errorf("three-language fixture step omitted %q", required)
		}
	}
	python := requireHAStep(t, readHAWorkflow(t)["python"], "Test public Python fault-probe and durable checkpoints")
	if python.If != "" || !strings.Contains(python.Run, "test-management-sdk-runner") {
		t.Fatal("Python probe regressions must execute in the Python check")
	}
}
