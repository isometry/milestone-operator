/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package version

import (
	"encoding/json"
	"runtime"
	"runtime/debug"
	"testing"
)

const (
	stampedVersion = "v1.2.3"
	stampedCommit  = "abc1234"
	stampedDate    = "2026-01-01T00:00:00Z"
	buildRevision  = "deadbeef"
	goVer          = "go1.26.3"
	goPlatform     = "linux/amd64"
)

func stub(t *testing.T, v, c, d string, bi *debug.BuildInfo) {
	t.Helper()
	oldV, oldC, oldD, oldRead := Version, Commit, Date, readBuildInfo
	Version, Commit, Date = v, c, d
	readBuildInfo = func() (*debug.BuildInfo, bool) { return bi, bi != nil }
	t.Cleanup(func() { Version, Commit, Date, readBuildInfo = oldV, oldC, oldD, oldRead })
}

func buildInfo(mainVersion, revision, vcsTime string) *debug.BuildInfo {
	bi := &debug.BuildInfo{Main: debug.Module{Version: mainVersion}}
	if revision != "" {
		bi.Settings = append(bi.Settings, debug.BuildSetting{Key: "vcs.revision", Value: revision})
	}
	if vcsTime != "" {
		bi.Settings = append(bi.Settings, debug.BuildSetting{Key: "vcs.time", Value: vcsTime})
	}
	return bi
}

func TestGet_Precedence(t *testing.T) {
	tests := []struct {
		name                string
		v, c, d             string
		bi                  *debug.BuildInfo
		wantV, wantC, wantD string
	}{
		{
			name:  "ldflags win over build info",
			v:     stampedVersion,
			c:     stampedCommit,
			d:     stampedDate,
			bi:    buildInfo("v9.9.9", buildRevision, "2020-02-02T00:00:00Z"),
			wantV: stampedVersion, wantC: stampedCommit, wantD: stampedDate,
		},
		{
			name:  "build info fills unset fields",
			bi:    buildInfo("v9.9.9", buildRevision, "2020-02-02T00:00:00Z"),
			wantV: "v9.9.9", wantC: buildRevision, wantD: "2020-02-02T00:00:00Z",
		},
		{
			name:  "fields fall back independently",
			v:     stampedVersion,
			bi:    buildInfo("v9.9.9", buildRevision, ""),
			wantV: stampedVersion, wantC: buildRevision,
		},
		{
			name:  "devel main version is ignored",
			bi:    buildInfo("(devel)", buildRevision, ""),
			wantV: unknown, wantC: buildRevision,
		},
		{
			name:  "empty main version under go test falls to dev",
			bi:    buildInfo("", "", ""),
			wantV: unknown,
		},
		{
			name:  "no build info at all",
			wantV: unknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub(t, tt.v, tt.c, tt.d, tt.bi)
			got := Get()
			if got.Version != tt.wantV || got.Commit != tt.wantC || got.Date != tt.wantD {
				t.Errorf("Get() = %+v, want version=%q commit=%q date=%q", got, tt.wantV, tt.wantC, tt.wantD)
			}
			if got.GoVersion != runtime.Version() || got.Platform != runtime.GOOS+"/"+runtime.GOARCH {
				t.Errorf("Get() runtime fields = %q, %q", got.GoVersion, got.Platform)
			}
		})
	}
}

func TestInfo_String(t *testing.T) {
	tests := []struct {
		name string
		in   Info
		want string
	}{
		{
			name: "full",
			in: Info{Version: stampedVersion, Commit: stampedCommit, Date: stampedDate,
				GoVersion: goVer, Platform: goPlatform},
			want: "v1.2.3 (commit abc1234, built 2026-01-01T00:00:00Z, go1.26.3 linux/amd64)",
		},
		{
			name: "long commit is abbreviated",
			in: Info{Version: stampedVersion, Commit: "2d1b83df9caf72dfb82702313d769e34e33a6e28",
				GoVersion: goVer, Platform: goPlatform},
			want: "v1.2.3 (commit 2d1b83d, go1.26.3 linux/amd64)",
		},
		{
			name: "no commit",
			in:   Info{Version: stampedVersion, Date: stampedDate, GoVersion: goVer, Platform: goPlatform},
			want: "v1.2.3 (built 2026-01-01T00:00:00Z, go1.26.3 linux/amd64)",
		},
		{
			name: "no date",
			in:   Info{Version: stampedVersion, Commit: stampedCommit, GoVersion: goVer, Platform: goPlatform},
			want: "v1.2.3 (commit abc1234, go1.26.3 linux/amd64)",
		},
		{
			name: "runtime only",
			in:   Info{Version: unknown, GoVersion: goVer, Platform: goPlatform},
			want: "dev (go1.26.3 linux/amd64)",
		},
		{
			name: "version only",
			in:   Info{Version: unknown},
			want: unknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestInfo_JSONShape(t *testing.T) {
	full, err := json.Marshal(Info{Version: "v1", Commit: "c", Date: "d", GoVersion: "go", Platform: "p"})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"version":"v1","commit":"c","date":"d","goVersion":"go","platform":"p"}`; string(full) != want {
		t.Errorf("json = %s, want %s", full, want)
	}
	sparse, err := json.Marshal(Info{Version: "v1", GoVersion: "go", Platform: "p"})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"version":"v1","goVersion":"go","platform":"p"}`; string(sparse) != want {
		t.Errorf("json = %s, want %s", sparse, want)
	}
}
