package flexihash

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
)

func TestLookup(t *testing.T) {
	data, err := ioutil.ReadFile("test_data")
	if err != nil {
		t.Error(err)
	}
	type args struct {
		res     string
		targets []string
	}
	type testCase struct {
		name string
		args args
		want string
	}
	var tests []testCase
	for i, value := range strings.Split(string(data), "\n") {
		rowParts := strings.Split(value, " ")
		if len(rowParts) != 2 {
			continue
		}
		key := rowParts[0]
		value := rowParts[1]
		tests = append(tests, testCase{
			fmt.Sprintf("%d", i),
			args{
				res:     key,
				targets: []string{"redis2", "redis3"},
			},
			value,
		})
	}
	for _, tt := range tests {
		got, err := Lookup(tt.args.res, tt.args.targets)
		if err != nil {
			t.Error(err)
		}
		if got != tt.want {
			t.Errorf("Lookup(%v, %v) = %v, want %v", tt.args.res, tt.args.targets, got, tt.want)
		}
	}
}
