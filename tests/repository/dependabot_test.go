package repository_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"sigs.k8s.io/yaml"
)

// These are repository policy contracts, not a replacement for GitHub's schema
// validation. Reuse the workspace YAML parser without adding a test dependency.
type dependabotConfig struct {
	Version int `json:"version"`
	Updates []struct {
		Ecosystem    string   `json:"package-ecosystem"`
		Directory    string   `json:"directory"`
		Directories  []string `json:"directories"`
		PullRequests int      `json:"open-pull-requests-limit"`
		TargetBranch string   `json:"target-branch"`
		Schedule     struct {
			Interval string `json:"interval"`
			Day      string `json:"day"`
			Time     string `json:"time"`
			Timezone string `json:"timezone"`
		} `json:"schedule"`
		Cooldown struct {
			DefaultDays int `json:"default-days"`
		} `json:"cooldown"`
		Allow []struct {
			Name        string   `json:"dependency-name"`
			UpdateTypes []string `json:"update-types"`
		} `json:"allow"`
		Ignore []any `json:"ignore"`
		Groups map[string]struct {
			AppliesTo       string   `json:"applies-to"`
			Patterns        []string `json:"patterns"`
			ExcludePatterns []string `json:"exclude-patterns"`
			UpdateTypes     []string `json:"update-types"`
			DependencyType  string   `json:"dependency-type"`
		} `json:"groups"`
	} `json:"updates"`
}

func readDependabot(t *testing.T) dependabotConfig {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", ".github", "dependabot.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var config dependabotConfig
	if err := yaml.UnmarshalStrict(data, &config); err != nil {
		t.Fatal(err)
	}
	if config.Version != 2 {
		t.Fatalf("Dependabot config version = %d, want 2", config.Version)
	}
	return config
}

func TestDependabotCoversMaintainedManifests(t *testing.T) {
	want := map[string][]string{
		"cargo":          {"/"},
		"gomod":          {"/"},
		"npm":            {"/"}, // The root lockfile covers the pnpm workspace.
		"pip":            {"/sdk/python"},
		"maven":          {"/sdk/java", "/tests/compatibility/java"},
		"github-actions": {"/"},
	}
	config := readDependabot(t)
	if len(config.Updates) != len(want) {
		t.Fatalf("got %d update entries, want one per maintained ecosystem (%d)", len(config.Updates), len(want))
	}
	for _, update := range config.Updates {
		expected, ok := want[update.Ecosystem]
		if !ok {
			t.Fatalf("unexpected or duplicate ecosystem %q", update.Ecosystem)
		}
		delete(want, update.Ecosystem)
		directories := slices.Clone(update.Directories)
		if update.Directory != "" {
			if len(directories) != 0 {
				t.Fatalf("%s sets both directory and directories", update.Ecosystem)
			}
			directories = []string{update.Directory}
		}
		slices.Sort(directories)
		if !slices.Equal(directories, expected) {
			t.Errorf("%s directories = %v, want %v", update.Ecosystem, directories, expected)
		}
	}
}

func TestDependabotRoutineUpdatesStayBounded(t *testing.T) {
	days := make(map[string]bool)
	for _, update := range readDependabot(t).Updates {
		t.Run(update.Ecosystem, func(t *testing.T) {
			wantPullRequests := 1
			if update.Ecosystem == "gomod" {
				// Kubernetes 0.x minor releases are API migration lines even
				// though Dependabot classifies them as SemVer minor updates.
				// Keep one slot available for ordinary Go updates while that
				// separately grouped migration is being reviewed.
				wantPullRequests = 2
			}
			if update.PullRequests != wantPullRequests {
				t.Errorf("routine PR limit = %d, want %d", update.PullRequests, wantPullRequests)
			}
			if update.Schedule.Interval != "weekly" || update.Schedule.Time != "02:00" || update.Schedule.Timezone != "Asia/Kolkata" {
				t.Error("routine updates must retain the weekly 02:00 Asia/Kolkata schedule")
			}
			if !slices.Contains([]string{"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}, update.Schedule.Day) || days[update.Schedule.Day] {
				t.Errorf("missing, invalid, or overlapping weekly day %q", update.Schedule.Day)
			}
			days[update.Schedule.Day] = true
			if update.Cooldown.DefaultDays != 7 {
				t.Errorf("routine cooldown = %d days, want 7", update.Cooldown.DefaultDays)
			}
			levels := []string{"minor", "patch"}
			allowed := []string{"version-update:semver-minor", "version-update:semver-patch"}
			if update.Ecosystem == "cargo" {
				levels = levels[1:]
				allowed = allowed[1:]
			}
			exclusions := []string(nil)
			if update.Ecosystem == "gomod" {
				exclusions = []string{"k8s.io/*", "sigs.k8s.io/*"}
			}
			group, ok := update.Groups["compatible-updates"]
			if !ok || group.AppliesTo != "version-updates" || !slices.Equal(group.Patterns, []string{"*"}) || !slices.Equal(group.UpdateTypes, levels) || !slices.Equal(group.ExcludePatterns, exclusions) || group.DependencyType != "" {
				t.Error("routine updates must share the compatible group at the approved SemVer levels")
			}
			if len(update.Allow) != 1 || update.Allow[0].Name != "*" || !slices.Equal(update.Allow[0].UpdateTypes, allowed) {
				t.Error("allow must restrict routine versions, including unmatched major updates")
			}
		})
	}
}

func TestDependabotIsolatesKubernetesReleaseLineMigrations(t *testing.T) {
	for _, update := range readDependabot(t).Updates {
		if update.Ecosystem != "gomod" {
			continue
		}
		patterns := []string{"k8s.io/*", "sigs.k8s.io/*"}
		group, ok := update.Groups["kubernetes-release-line"]
		if !ok || group.AppliesTo != "version-updates" || !slices.Equal(group.Patterns, patterns) || !slices.Equal(group.UpdateTypes, []string{"minor", "patch"}) {
			t.Fatal("Go updates must isolate the coordinated Kubernetes release line")
		}
		compatible := update.Groups["compatible-updates"]
		if !slices.Equal(compatible.ExcludePatterns, patterns) {
			t.Fatal("ordinary Go updates must exclude Kubernetes release-line packages")
		}
		return
	}
	t.Fatal("Go Dependabot entry is missing")
}

func TestDependabotSecurityUpdatesRemainUnfiltered(t *testing.T) {
	for _, update := range readDependabot(t).Updates {
		t.Run(update.Ecosystem, func(t *testing.T) {
			if update.TargetBranch != "" || len(update.Ignore) != 0 {
				t.Error("do not redirect updates off the default branch or suppress security fixes with ignore rules")
			}
			group, ok := update.Groups["security-updates"]
			if !ok || group.AppliesTo != "security-updates" || !slices.Equal(group.Patterns, []string{"*"}) || len(group.UpdateTypes) != 0 || len(group.ExcludePatterns) != 0 || group.DependencyType != "" {
				t.Error("security updates must retain a separate all-dependency group without version/type exclusions")
			}
		})
	}
}
