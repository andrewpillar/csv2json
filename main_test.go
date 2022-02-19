package main

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func checkCsv(t *testing.T, expected io.Reader, actual string) {
	records := make([]map[string]interface{}, 0)

	sc := bufio.NewScanner(expected)

	for sc.Scan() {
		m := make(map[string]interface{})

		b := sc.Bytes()

		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		records = append(records, m)
	}

	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(actual)

	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()

	sc = bufio.NewScanner(f)

	i := 0

	for sc.Scan() {
		m := make(map[string]interface{})

		b := sc.Bytes()

		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}

		rec := records[i]

		if l := len(m); l != len(rec) {
			t.Fatalf("%s - unexpected number of columns, expected=%d, got=%d\n", actual, len(rec), l)
		}

		for k, v := range rec {
			v2, ok := m[k]

			if !ok {
				t.Fatalf("%s - could not find column %q\n", actual, k)
			}

			typ := reflect.TypeOf(v)
			typ2 := reflect.TypeOf(v2)

			kind := typ.Kind()
			kind2 := typ2.Kind()

			if kind != kind2 {
				t.Fatalf("%s - unexpected column type for column %q, expected=%q, got=%q\n", actual, k, kind, kind2)
			}

			if !reflect.DeepEqual(v, v2) {
				t.Fatalf("%s - unexpected column value for column %q, expected=%v, got=%v\n", actual, k, v, v2)
			}
		}
		i++
	}

	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
}

func Test_Main(t *testing.T) {
	tests := []struct {
		csvfile    string
		schemafile string
		goldfile   string
	}{
		{
			filepath.Join("testdata", "users.csv"),
			filepath.Join("testdata", "users.schema"),
			filepath.Join("testdata", "users.golden"),
		},
		{
			filepath.Join("testdata", "ips.csv"),
			filepath.Join("testdata", "ips.schema"),
			filepath.Join("testdata", "ips.golden"),
		},
		{
			filepath.Join("testdata", "numbers.csv"),
			filepath.Join("testdata", "numbers.schema"),
			filepath.Join("testdata", "numbers.golden"),
		},
		{
			filepath.Join("testdata", "numbers2.csv"),
			filepath.Join("testdata", "numbers2.schema"),
			filepath.Join("testdata", "numbers2.golden"),
		},
	}

	for i, test := range tests {
		if err := run([]string{"csv2json", "-s", test.schemafile, test.csvfile}); err != nil {
			t.Fatalf("tests[%d] - %s\n", i, err)
		}

		func() {
			f, err := os.Open(test.goldfile)

			if err != nil {
				t.Fatalf("tests[%d] - %s\n", i, err)
			}

			defer f.Close()

			outname := filepath.Base(test.csvfile)
			outname = outname[:len(outname)-4] + ".json"

			checkCsv(t, f, outname)
			os.RemoveAll(outname)
		}()
	}
}
