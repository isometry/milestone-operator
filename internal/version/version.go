/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package version reports build identity shared by the manager and
// milestonectl.
package version

import (
	"runtime"
	"runtime/debug"
	"strings"
)

// Stamped at link time with
// -ldflags "-X github.com/isometry/milestone-operator/internal/version.Version=v1.2.3 ...".
// Version is v-prefixed by convention, matching git tags and "go install".
var (
	Version string
	Commit  string
	Date    string
)

// unknown is reported when neither the linker nor the toolchain supplied a version.
const unknown = "dev"

const shortCommitLen = 7

// readBuildInfo is a variable so tests can stub it: under "go test" the
// toolchain records no main module version.
var readBuildInfo = debug.ReadBuildInfo

type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit,omitempty"`
	Date      string `json:"date,omitempty"`
	GoVersion string `json:"goVersion"`
	Platform  string `json:"platform"`
}

// Get prefers the linker-stamped values and falls back to what the Go
// toolchain recorded, so "go install" builds still identify themselves.
func Get() Info {
	i := Info{
		Version:   Version,
		Commit:    Commit,
		Date:      Date,
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS + "/" + runtime.GOARCH,
	}
	if bi, ok := readBuildInfo(); ok {
		if i.Version == "" && bi.Main.Version != "(devel)" {
			i.Version = bi.Main.Version
		}
		for _, s := range bi.Settings {
			switch {
			case s.Key == "vcs.revision" && i.Commit == "":
				i.Commit = s.Value
			case s.Key == "vcs.time" && i.Date == "":
				i.Date = s.Value
			}
		}
	}
	if i.Version == "" {
		i.Version = unknown
	}
	return i
}

// String renders "v1.2.3 (commit abc1234, built 2026-01-01T00:00:00Z, go1.26.3 linux/amd64)",
// omitting whatever is unknown.
func (i Info) String() string {
	var parts []string
	if i.Commit != "" {
		// The line is for humans; JSON output keeps the full SHA.
		parts = append(parts, "commit "+i.Commit[:min(len(i.Commit), shortCommitLen)])
	}
	if i.Date != "" {
		parts = append(parts, "built "+i.Date)
	}
	if rt := strings.TrimSpace(i.GoVersion + " " + i.Platform); rt != "" {
		parts = append(parts, rt)
	}
	if len(parts) == 0 {
		return i.Version
	}
	return i.Version + " (" + strings.Join(parts, ", ") + ")"
}
