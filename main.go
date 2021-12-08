package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// splitspace slices p into all substrings separated by any number of spaces
// or tabs. Spaces or tabs wrapped in double-quotes are preserved.
//
// For example, the given string
//
//     `     "Hello, world"     [0-9]+   foo    `
//
// would be sliced into,
//
//    ["Hello, world", "[0-9]+", "foo"]
//
// double-quotes used to preserved spaces or tabs are dropped in the final
// slice.
func splitspace(p []byte) []string {
	a := make([]string, 0, 5)

	var (
		i int
		r rune
		w int

		quoted bool
		trim   bool
		start  int = -1
	)

	for i < len(p) {
		r = rune(p[i])
		w = 1

		if r >= utf8.RuneSelf {
			r, w = utf8.DecodeRune(p[i:])
		}

		if r != ' ' && r != '\t' {
			if !quoted && start < 0 {
				start = i
				continue
			}
		}

		i += w

		if r == '"' {
			quoted = !quoted
			trim = true
		}

		if r == ' ' || r == '\t' {
			if !quoted && start >= 0 {
				if trim {
					start += 1
					i -= 1
					trim = false
				}
				a = append(a, string(p[start:i-w]))
				start = -1
			}
		}
	}

	if start > 0 {
		if trim {
			start += 1
			i -= 1
			trim = false
		}
		a = append(a, string(p[start:i]))
	}
	return a
}

type Value interface {
	Format(fmt string)

	MarshalJSON() ([]byte, error)
}

type UnmarshalFunc func(s string) (Value, error)

type UnmarshalError struct {
	Type string
	Err  error
}

func (e UnmarshalError) Error() string {
	return e.Type + " " + e.Err.Error()
}

type String struct {
	re   *regexp.Regexp
	s    string
	repl string
}

func (s *String) Format(repl string) {
	s.repl = repl
}

func (s *String) MarshalJSON() ([]byte, error) {
	if s.repl != "" {
		s.s = s.re.ReplaceAllString(s.s, s.repl)
	}
	return json.Marshal(s.s)
}

func UnmarshalString(re *regexp.Regexp) UnmarshalFunc {
	return func(s string) (Value, error) {
		if re != nil {
			if !re.Match([]byte(s)) {
				return nil, UnmarshalError{
					Type: "string",
					Err:  fmt.Errorf("%q does not match pattern %q", s, re.String()),
				}
			}
		}
		return &String{re: re, s: s}, nil
	}
}

type Int struct {
	n int
}

func (i *Int) Format(_ string) {}

func (i *Int) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.n)
}

func UnmarshalInt(base int) UnmarshalFunc {
	return func(s string) (Value, error) {
		n, err := strconv.ParseInt(s, base, 64)

		if err != nil {
			return nil, UnmarshalError{Type: "int", Err: err}
		}
		return &Int{n: int(n)}, nil
	}
}

type Float struct {
	n float64
}

func (f *Float) Format(_ string) {}

func (f *Float) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.n)
}

func UnmarshalFloat(s string) (Value, error) {
	n, err := strconv.ParseFloat(s, 64)

	if err != nil {
		return nil, UnmarshalError{Type: "float", Err: err}
	}
	return &Float{n: n}, nil
}

type Time struct {
	t      time.Time
	layout string
}

func (t *Time) Format(fmt string) { t.layout = fmt }

func (t *Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.t.Format(t.layout))
}

func UnmarshalTime(layout string) UnmarshalFunc {
	return func(s string) (Value, error) {
		t, err := time.Parse(layout, s)

		if err != nil {
			return nil, UnmarshalError{Type: "time", Err: err}
		}
		return &Time{t: t, layout: time.RFC3339}, nil
	}
}

type SchemaRecord struct {
	Outfmt    string
	Dest      string
	Unmarshal UnmarshalFunc
}

type Schema struct {
	mu   *sync.RWMutex
	recs map[string]SchemaRecord
}

func NewSchema() *Schema {
	return &Schema{
		mu:   &sync.RWMutex{},
		recs: make(map[string]SchemaRecord),
	}
}

func (s *Schema) Add(name string, rec SchemaRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.recs[name] = rec
}

func (s *Schema) Get(name string) (SchemaRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.recs[name]

	return rec, ok
}

type SchemaDecodeError struct {
	File string
	Line int
	Err  error
}

func (e SchemaDecodeError) Error() string {
	return e.File + ":" + strconv.FormatInt(int64(e.Line), 10) + " - " + e.Err.Error()
}

func parsebase(s string) (int, error) {
	n, err := strconv.ParseInt(s, 10, 64)

	if err != nil {
		return 0, err
	}

	bases := map[int64]struct{}{
		0:  {}, // valid for strconv.ParseInt
		2:  {},
		8:  {},
		10: {},
		16: {},
	}

	if _, ok := bases[n]; !ok {
		return 0, fmt.Errorf("invalid base %d", n)
	}
	return int(n), nil
}

func (s *Schema) Load(fname string) error {
	f, err := os.Open(fname)

	if err != nil {
		return err
	}

	defer f.Close()

	sc := bufio.NewScanner(f)

	// Table to store any previously compiled regex.
	retab := make(map[string]*regexp.Regexp)

	line := 0

	for sc.Scan() {
		line++

		p := sc.Bytes()

		if p[0] == '#' {
			continue
		}

		parts := splitspace(p)

		if len(parts) < 2 {
			return SchemaDecodeError{
				File: fname,
				Line: line,
				Err:  errors.New("too few columns in schema record"),
			}
		}

		col := parts[0]
		typ := parts[1]
		pat := "_"
		fmt := ""
		dst := col

		if len(parts) >= 3 {
			pat = parts[2]

			if len(parts) >= 4 {
				fmt = parts[3]

				if len(parts) >= 5 {
					dst = parts[4]
				}
			}
		}

		var unmarshal UnmarshalFunc

		switch typ {
		case "string":
			var re *regexp.Regexp

			if pat != "_" {
				var ok bool

				re, ok = retab[pat]

				if !ok {
					var err error

					re, err = regexp.Compile(pat)

					if err != nil {
						return SchemaDecodeError{
							File: fname,
							Line: line,
							Err:  err,
						}
					}
				}
				unmarshal = UnmarshalString(re)
			}
		case "int":
			base := 10

			if pat != "_" {
				n, err := parsebase(pat)

				if err != nil {
					return SchemaDecodeError{
						File: fname,
						Line: line,
						Err:  err,
					}
				}
				base = n
			}
			unmarshal = UnmarshalInt(base)
		case "float":
			unmarshal = UnmarshalFloat
		case "time":
			if pat == "_" {
				pat = time.RFC3339
			}
			unmarshal = UnmarshalTime(pat)
		default:
			return SchemaDecodeError{
				File: fname,
				Line: line,
				Err:  errors.New("unknown schema type " + typ),
			}
		}

		s.Add(col, SchemaRecord{
			Outfmt:    fmt,
			Dest:      dst,
			Unmarshal: unmarshal,
		})
	}

	if err := sc.Err(); err != nil {
		return err
	}
	return nil
}

type pos struct {
	line, col int
}

type Parser struct {
	csv    *csv.Reader
	schema *Schema
	errh   func(int, int, string)

	headers []string // first line of the csv file

	// The current record and position in that record is tracked so we can have
	// an accurate position when emitting errors to the error handler.
	record []string // current csv record we've scanned
	recpos int      // position in the record we've scanner

	pos pos // line and colum position in the stream, incremented each time we
	// scan in a record, or retrieve a column from a scanned record.
	errc int
}

func NewParser(in io.Reader, delim rune, schema *Schema, errh func(int, int, string)) (*Parser, error) {
	p := &Parser{
		csv:    csv.NewReader(in),
		schema: schema,
		errh:   errh,
	}

	p.csv.Comma = delim

	if err := p.init(); err != nil {
		return nil, err
	}
	return p, nil
}

// nextrecord reads in the next record from the underlying input stream.
func (p *Parser) nextrecord() error {
	record, err := p.csv.Read()

	if err != nil {
		return err
	}

	p.record = record
	p.recpos = 0

	p.pos.line++
	p.pos.col = 1

	return nil
}

// next returns the column name, and value of the next column in the current
// record the parser has scanned in.
func (p *Parser) next() (string, string) {
	if p.recpos >= len(p.record) {
		return "", ""
	}

	hdr := p.headers[p.recpos]
	val := p.record[p.recpos]

	// Width of column value to increment column position by.
	w := len(val)

	if w == 0 {
		w = 1
	}

	p.recpos++
	p.pos.col += w

	return hdr, val
}

// init will initialize the parser by reading the first line in the underlying
// input stream and using that as the header.
func (p *Parser) init() error {
	if err := p.nextrecord(); err != nil {
		return nil
	}

	p.headers = p.record
	return nil
}

func (p *Parser) err(err error) {
	p.errc++
	p.errh(p.pos.line, p.pos.col, err.Error())
}

func unmarshalAny(s string) (Value, error) {
	funcs := []UnmarshalFunc{
		UnmarshalInt(10),
		UnmarshalFloat,
		UnmarshalTime(time.RFC3339),
		UnmarshalString(nil),
	}

	for _, fn := range funcs {
		if v, err := fn(s); err == nil {
			return v, nil
		}
	}
	return &String{s: s}, nil
}

type ColumnError struct {
	Col string
	Err error
}

func (e ColumnError) Error() string {
	return e.Col + ": " + e.Err.Error()
}

func (p *Parser) json() ([]byte, error) {
	m := make(map[string]Value)

	for {
		col, val := p.next()

		if val == "" {
			if col == "" {
				break
			}
			continue
		}

		rec, ok := p.schema.Get(col)

		if !ok {
			rec = SchemaRecord{
				Dest:      col,
				Unmarshal: unmarshalAny,
			}
		}

		v, err := rec.Unmarshal(val)

		if err != nil {
			return nil, ColumnError{
				Col: col,
				Err: err,
			}
		}

		if rec.Outfmt != "" {
			v.Format(rec.Outfmt)
		}
		m[rec.Dest] = v
	}
	return json.Marshal(m)
}

func (p *Parser) Parse(out io.Writer) error {
	for {
		if err := p.nextrecord(); err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			break
		}

		b, err := p.json()

		if err != nil {
			p.err(err)
			continue
		}

		if _, err := out.Write(append(b, '\n')); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	argv0 := os.Args[0]

	var (
		schema string
		delim  string
	)

	fs := flag.NewFlagSet(argv0, flag.ExitOnError)
	fs.StringVar(&schema, "s", "", "the schema file to use")
	fs.StringVar(&delim, "d", ",", "the csv delimeter")
	fs.Parse(os.Args[1:])

	d, _ := utf8.DecodeRuneInString(delim)

	if d == utf8.RuneError {
		fmt.Fprintf(os.Stderr, "%s: invalid utf-8 character for delimeter, must be single character\n", argv0)
		os.Exit(1)
	}

	args := fs.Args()

	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "%s [-d delim, -s schema] <file,...>\n", argv0)
		os.Exit(1)
	}

	s := NewSchema()

	if schema != "" {
		s.Load(schema)
	}

	sems := make(chan struct{}, runtime.GOMAXPROCS(0)+10)
	errs := make(chan error)

	wg := sync.WaitGroup{}
	wg.Add(len(args))

	for _, fname := range args {
		errh := func(line, col int, msg string) {
			fmt.Fprintf(os.Stderr, "%s,%d:%d - %s\n", fname, line, col, msg)
		}

		go func(fname string) {
			sems <- struct{}{}

			defer func() {
				wg.Done()
				<-sems
			}()

			f, err := os.Open(fname)

			if err != nil {
				errs <- err
				return
			}

			defer f.Close()

			outname := f.Name()

			if strings.HasSuffix(outname, ".csv") {
				outname = outname[:len(outname)-4]
			}
			outname += ".json"

			out, err := os.OpenFile(outname, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(0644))

			if err != nil {
				errs <- err
				return
			}

			defer out.Close()

			p, err := NewParser(f, d, s, errh)

			if err != nil {
				errs <- err
				return
			}

			if err := p.Parse(out); err != nil {
				errs <- err
				return
			}
			fmt.Println(outname)
		}(fname)
	}

	go func() {
		wg.Wait()
		close(errs)
	}()

	code := 0

	for err := range errs {
		fmt.Fprintf(os.Stderr, "%s: %s\n", argv0, err)
		code = 1
	}

	if code != 0 {
		os.Exit(code)
	}
}
