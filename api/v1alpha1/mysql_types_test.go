package v1alpha1

import "testing"

func TestFailoverEnabled(t *testing.T) {
	one := int32(1)
	three := int32(3)
	f := false
	tr := true

	cases := []struct {
		name string
		spec MySQLSpec
		want bool
	}{
		{"standalone", MySQLSpec{Replicas: &one}, false},
		{"ha default", MySQLSpec{Replicas: &three}, true},
		{"ha explicit off", MySQLSpec{Replicas: &three, Failover: &FailoverSpec{Enabled: &f}}, false},
		{"ha explicit on", MySQLSpec{Replicas: &three, Failover: &FailoverSpec{Enabled: &tr}}, true},
	}
	for _, tc := range cases {
		if got := tc.spec.FailoverEnabled(); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestEffectivePrimaryPod(t *testing.T) {
	m := &MySQL{}
	m.Name = "db"
	if m.EffectivePrimaryPod() != "db-0" {
		t.Fatalf("default primary: %s", m.EffectivePrimaryPod())
	}
	m.Status.PrimaryPod = "db-2"
	if m.EffectivePrimaryPod() != "db-2" {
		t.Fatalf("status primary: %s", m.EffectivePrimaryPod())
	}
}
