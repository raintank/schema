package schema

import (
	"testing"
)

func BenchmarkSetId(b *testing.B) {
	metric := MetricData{
		OrgId:    1234,
		Name:     "key1=val1.key2=val2.my.test.metric.name",
		Metric:   "my.test.metric.name",
		Interval: 15,
		Value:    0.1234,
		Unit:     "ms",
		Time:     1234567890,
		Mtype:    "gauge",
		Tags:     []string{"key1:val1", "key2:val2"},
	}
	for i := 0; i < b.N; i++ {
		metric.SetId()
	}
}

func TestTagValidation(t *testing.T) {
	type testCase struct {
		tag       []string
		expecting bool
	}

	testCases := []testCase{
		{[]string{"abc=cba"}, true},
		{[]string{"a="}, false},
		{[]string{"a!="}, false},
		{[]string{"=abc"}, false},
		{[]string{"@#$%!=(*&"}, false},
		{[]string{"!@#$%=(*&"}, false},
		{[]string{"@#;$%=(*&"}, false},
		{[]string{"@#$%=(;*&"}, false},
		{[]string{"@#$%=(*&"}, true},
		{[]string{"@#$%=(*&", "abc=!fd", "a===="}, true},
		{[]string{"@#$%=(*&", "abc=!fd", "a===;="}, false},
	}

	for _, tc := range testCases {
		if tc.expecting != validateTags(tc.tag) {
			t.Fatalf("Expected %t, but testcase %s returned %t", tc.expecting, tc.tag, !tc.expecting)
		}
	}
}

func TestNameWithTags(t *testing.T) {
	type testCase struct {
		expectedName string
		md           MetricDefinition
	}

	testCases := []testCase{
		{
			"a.b.c;tag1=value1",
			MetricDefinition{Name: "a.b.c", Tags: []string{"tag1=value1", "name=ccc"}},
		}, {
			"a.b.c;a=a;b=b;c=c",
			MetricDefinition{Name: "a.b.c", Tags: []string{"name=a.b.c", "c=c", "b=b", "a=a"}},
		}, {
			"a.b.c",
			MetricDefinition{Name: "a.b.c", Tags: []string{"name=a.b.c"}},
		}, {
			"a.b.c",
			MetricDefinition{Name: "a.b.c", Tags: []string{}},
		}, {
			"a.b.c;a=a;b=b;c=c",
			MetricDefinition{Name: "a.b.c", Tags: []string{"c=c", "a=a", "b=b"}},
		},
	}

	for _, tc := range testCases {
		tc.md.SetId()
		fullName := tc.md.NameWithTags()
		if tc.expectedName != fullName {
			t.Fatalf("Expected name %s, but got %s", tc.expectedName, fullName)
		}
	}
}
