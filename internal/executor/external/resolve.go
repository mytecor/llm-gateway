package external

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

var (
	buildMu    sync.Mutex
	builtPaths = make(map[string]string)
)

// ResolveDriverBinary locates the executable for a named driver (<name>-driver)
// across environment overrides, adjacent directories, standard build outputs,
// and $PATH. Source builds require explicit LLM_GATEWAY_DEV_BUILD=1; production
// startup never invokes a compiler or downloads dependencies.
func ResolveDriverBinary(name string) (string, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return "", fmt.Errorf("driver name is required")
	}

	envKey := "LLM_GATEWAY_DRIVER_" + strings.ToUpper(name)
	if override := strings.TrimSpace(os.Getenv(envKey)); override != "" {
		if info, err := os.Stat(override); err == nil && !info.IsDir() {
			return override, nil
		}
		if path, err := exec.LookPath(override); err == nil {
			return path, nil
		}
		return "", fmt.Errorf("%s=%q not found", envKey, override)
	}

	binaryNames := []string{
		name + "-driver",
		"llm-gateway-driver-" + name,
	}

	var searchDirs []string
	if dir := strings.TrimSpace(os.Getenv("LLM_GATEWAY_DRIVERS_DIR")); dir != "" {
		searchDirs = append(searchDirs, dir)
	}
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		searchDirs = append(searchDirs, exeDir, filepath.Join(exeDir, "drivers"))
	}
	if cwd, err := os.Getwd(); err == nil {
		searchDirs = append(searchDirs,
			filepath.Join(cwd, "bin"),
			filepath.Join(cwd, "drivers", name),
			filepath.Join(cwd, "drivers", name, "target", "release"),
			filepath.Join(cwd, "drivers", name, "target", "debug"),
		)
	}
	repoRoot := findRepoRoot()
	if repoRoot != "" {
		searchDirs = append(searchDirs,
			filepath.Join(repoRoot, "bin"),
			filepath.Join(repoRoot, "drivers", name),
			filepath.Join(repoRoot, "drivers", name, "target", "release"),
			filepath.Join(repoRoot, "drivers", name, "target", "debug"),
		)
	}

	for _, dir := range searchDirs {
		for _, binName := range binaryNames {
			candidate := filepath.Join(dir, binName)
			if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
				return candidate, nil
			}
		}
	}

	for _, binName := range binaryNames {
		if path, err := exec.LookPath(binName); err == nil {
			return path, nil
		}
	}

	if repoRoot != "" && os.Getenv("LLM_GATEWAY_DEV_BUILD") == "1" {
		driverModDir := filepath.Join(repoRoot, "drivers", name)
		if info, err := os.Stat(filepath.Join(driverModDir, "go.mod")); err == nil && !info.IsDir() {
			return buildGoDriverOnDemand(repoRoot, name, driverModDir)
		}
		if info, err := os.Stat(filepath.Join(driverModDir, "Cargo.toml")); err == nil && !info.IsDir() {
			return buildCargoDriverOnDemand(repoRoot, name, driverModDir)
		}
	}

	return "", fmt.Errorf("driver binary %q not found in PATH or bin/", name+"-driver")
}

// ResolveDriverCommand resolves the executable command for a provider given its
// driver name and optional command slice.
func ResolveDriverCommand(driver string, command []string) ([]string, error) {
	driver = strings.ToLower(strings.TrimSpace(driver))
	if driver == "" && len(command) == 0 {
		driver = "http"
	}
	if driver != "" {
		bin, err := ResolveDriverBinary(driver)
		if err != nil {
			return nil, fmt.Errorf("unknown driver %q: %w", driver, err)
		}
		return []string{bin}, nil
	}
	if len(command) == 0 || strings.TrimSpace(command[0]) == "" {
		return nil, fmt.Errorf("driver command is required")
	}
	if _, err := exec.LookPath(command[0]); err != nil {
		return nil, err
	}
	return append([]string(nil), command...), nil
}

func findRepoRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	dir := filepath.Dir(file)
	for i := 0; i < 6; i++ {
		if info, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil && !info.IsDir() {
			if dInfo, err := os.Stat(filepath.Join(dir, "drivers")); err == nil && dInfo.IsDir() {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

func buildGoDriverOnDemand(repoRoot, name, driverModDir string) (string, error) {
	buildMu.Lock()
	defer buildMu.Unlock()

	if cached, ok := builtPaths[name]; ok {
		if info, err := os.Stat(cached); err == nil && !info.IsDir() {
			return cached, nil
		}
	}

	outPath := filepath.Join(repoRoot, "bin", name+"-driver")
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return "", fmt.Errorf("prepare bin dir for %s-driver: %w", name, err)
	}
	cmd := exec.Command("go", "build", "-o", outPath, ".")
	cmd.Dir = driverModDir
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("build %s-driver: %w (%s)", name, err, strings.TrimSpace(string(out)))
	}
	builtPaths[name] = outPath
	return outPath, nil
}

func buildCargoDriverOnDemand(repoRoot, name, driverModDir string) (string, error) {
	buildMu.Lock()
	defer buildMu.Unlock()

	if cached, ok := builtPaths[name]; ok {
		if info, err := os.Stat(cached); err == nil && !info.IsDir() {
			return cached, nil
		}
	}

	cmd := exec.Command("cargo", "build", "--locked", "--manifest-path", filepath.Join(driverModDir, "Cargo.toml"))
	cmd.Dir = repoRoot
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("build %s-driver: %w (%s)", name, err, strings.TrimSpace(string(out)))
	}
	candidate := filepath.Join(driverModDir, "target", "debug", name+"-driver")
	if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
		builtPaths[name] = candidate
		return candidate, nil
	}
	return "", fmt.Errorf("built %s-driver binary not found at %s", name, candidate)
}
