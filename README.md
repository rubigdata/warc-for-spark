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
 |-- warc_id: string (nullable = false)
 |-- warc_type: string (nullable = false)
 |-- warc_target_uri: string (nullable = true)
 |-- warc_date: timestamp (nullable = false)
 |-- content_type: string (nullable = false)
 |-- warc_headers: map (nullable = false)
 |    |-- key: string
 |    |-- value: array (valueContainsNull = false)
 |    |    |-- element: string (containsNull = false)
 |-- warc_body: string (nullable = false)
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
 |-- warc_id: string (nullable = false)
 |-- warc_type: string (nullable = false)
 |-- warc_target_uri: string (nullable = true)
 |-- warc_date: timestamp (nullable = false)
 |-- content_type: string (nullable = false)
 |-- warc_headers: map (nullable = false)
 |    |-- key: string
 |    |-- value: array (valueContainsNull = false)
 |    |    |-- element: string (containsNull = false)
 |-- http_headers: map (nullable = true)
 |    |-- key: string
 |    |-- value: array (valueContainsNull = false)
 |    |    |-- element: string (containsNull = false)
 |-- http_body: string (nullable = true)
```

Predicate pushdown is supported for columns `warc_id`, `warc_type`, `warc_target_uri`, `warc_date` and `content_type`.

## Options

The WARC input format supports the following read options:

| Option name    | Type   | Description                                                                                                           |
|----------------|--------|-----------------------------------------------------------------------------------------------------------------------|
| parseHTTP      | bool   | Parses the WARC body as a HTTP response. Replaces the `warc_body` column with `http_headers` and `http_body` columns. |
| pathGlobFilter | string | Read only files with file names matching the given glob pattern.                                                      |
