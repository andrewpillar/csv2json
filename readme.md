# csv2json

csv2json is a simple program for converting CSV files to JSON. Each CSV file
given to csv2json will be written to a corresponding JSON file. It is expected
for the first line in each CSV file to be the column names for that file. If
any records cannot be converted to JSON for any reason, then these will simply
be skipped over.

* [Quick start](#quick-start)
* [Schema file](#schema-file)

## Quick start

Assume we have a file `users.csv`,

    $ cat users.csv
    id,username,password,created_at
    1,andrew,secret,07/12/2021
    2,sam,terces,08/12/2021

when given to csv2json, would produce a corresponding `users.json` file.

    $ csv2json users.csv
    users.json
    $ cat users.json
    {"created_at":"07/12/2021","id":1,"password":"secret","username":"andrew"}
    {"created_at":"08/12/2021","id":2,"password":"terces","username":"sam"}

The above example demonstrats how csv2json works in simple scenarios where you
want a 1-to-1 mapping of CSV to JSON. However, there may be instances where you
would prefer to validate, and perhaps transform the data you're converting.
This is possible with csv2json via schema files.

A schema file simply describes the type for each column, the pattern that
column's value should adhere to, and how the column's value should be output
to JSON. In the above example the `users.csv` file has a `created_at` column
in the format of `dd/mm/yyyy`, with a schema file we can tell csv2json to take
this field, treat it as a `time`, and convert it to an appropriate format.

    $ cat schema
    # Column    Type  Pattern     Format
    created_at  time  02/01/2006  2006-01-02T15:04:05Z

With the above schema file we can rerun csv2json, and give it a schema to use
via the `-s` flag.

    $ csv2json -s schema users.csv
    users.json
    $ cat users.json
	{"created_at":"2021-12-07T00:00:00Z","id":1,"password":"secret","username":"andrew"}
	{"created_at":"2021-12-08T00:00:00Z","id":2,"password":"terces","username":"sam"}

As you can see, with the schema file csv2json was able to convert the initial
time value in the CSV file into a more appropiate file. With the above schema
in place, an error would occur if any of the CSV records have a `created_at`
column that do not match the specified pattern. The schema file can be used for
some simple validation on the CSV data to ensure only the correct data is
converted to JSON.

## Schema file

The schema file defines the types for each column being convered in a CSV file,
along with the patterns the column's values should adhere to, and the output
format for the corresponding JSON value. The schema file is made up of space
delimited lines in the following format,

    # Comment line
    column  type  pattern  format  destination

**`column`** - required

The column field describes the name of the column in the CSV File. This is
required.

**`type`** - required

This describes the type of the column's value in the CSV file. This is required
and should be one of `string`, `bool`, `int`, `float`, or `time`.

**`pattern`**

This describes the input pattern of the column's value in the CSV file. This
will vary depending on the type of the column. Detailed below are the different
patterns that can be used for each type.

  * `string` - The pattern for a string is a regular expression. This will be
  applied to the input strings to the CSV file. Any strings that do not match
  the given pattern will be rejected, and an error will be reported.

  * `int` - The base for the integer being parsed. This can be either `0`,
  `2`, `8`, `10`, or `16`. By default numbers are parsed as base `10`. When
  set to `0`, the base is implied by the prefix of the string, for example,
  `0b` for binary, `0o` for octal, etc.

  * `time` -  The layout of the time in the CSV file. This uses the reference
  date `Mon Jan 2 15:04:05 MST 2006` from the standard library. For more
  information on how to use different date layouts see the Go documentation
  for the [time][time] package.

[time]: https://pkg.go.dev/time#pkg-constants

**`format`**

This describes the output format of the column's value when written to JSON.
This will vary depending on the column's type.

**`destination`**

This describes the name of the field that should be written to in the output
JSON. If not given, then the original CSV column name is used.
