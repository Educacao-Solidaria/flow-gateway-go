package version

import "testing"

func TestString(t *testing.T) {
	tests := []struct {
		name, version, commit, want string
	}{
		{"padrao de desenvolvimento", "dev", "none", "flow-gateway dev (none)"},
		{"release injetada", "v1.0.0", "a075a1c", "flow-gateway v1.0.0 (a075a1c)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldVersion, oldCommit := Version, Commit
			t.Cleanup(func() { Version, Commit = oldVersion, oldCommit })

			Version, Commit = tt.version, tt.commit
			if got := String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}
