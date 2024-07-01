# WARC4Spark

This repository contains a Spark data source for WARC files, based on [jwarc](https://github.com/iipc/jwarc).

The data source supports column pruning and filter (or predicate) pushdown to improve read efficiency.

## Usage

Using the data source is as simple as choosing `org.rubigdata.warc` as input format for a Spark DataFrame:

```scala
val df = spark
  .read
  .format("org.rubigdata.warc")
  .load(input)
``` 

The resulting DataFrame will have the following schema:

```
root
 |-- warcId: string (nullable = false)
 |-- warcType: string (nullable = false)
 |-- warcTargetUri: string (nullable = true)
 |-- warcDate: timestamp (nullable = false)
 |-- contentType: string (nullable = false)
 |-- warcHeaders: map (nullable = false)
 |    |-- key: string
 |    |-- value: array (valueContainsNull = false)
 |    |    |-- element: string (containsNull = false)
 |-- warcBody: string (nullable = false)
```

Similarly, if we want to parse the WARC record bodies as HTTP responses (for WARC records of type `response`), we can use:

```scala
val df = spark
  .read
  .format("org.rubigdata.warc")
  .option("parseHTTP", "true")
  .load(input)
```

This will result in a DataFrame with the following schema:

```
root
 |-- warcId: string (nullable = false)
 |-- warcType: string (nullable = false)
 |-- warcTargetUri: string (nullable = true)
 |-- warcDate: timestamp (nullable = false)
 |-- contentType: string (nullable = false)
 |-- warcHeaders: map (nullable = false)
 |    |-- key: string
 |    |-- value: array (valueContainsNull = false)
 |    |    |-- element: string (containsNull = false)
 |-- httpHeaders: map (nullable = false)
 |    |-- key: string
 |    |-- value: array (valueContainsNull = false)
 |    |    |-- element: string (containsNull = false)
 |-- httpBody: string (nullable = false)
```

Predicate pushdown is supported for columns `warcId`, `warcType`, `warcTargetUri`, `warcDate` and `contentType`.

## Options

The WARC input format supports the following read options:

| Option name    | Type   | Description                                                                                                        |
|----------------|--------|--------------------------------------------------------------------------------------------------------------------|
| lenient        | bool   | Enables lenient WARC parsing (e.g. allowing invalid UTF-8)                                                         |
| parseHTTP      | bool   | Parses the WARC body as a HTTP response. Replaces the `warcBody` column with `httpHeaders` and `httpBody` columns. |
| pathGlobFilter | string | Read only files with file names matching the given glob pattern.                                                   |
