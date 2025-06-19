# WARC4Spark

This repository contains a Spark data source for WARC files, based on [jwarc](https://github.com/iipc/jwarc).

The data source supports column pruning and filter (or predicate) pushdown to improve read efficiency, and is able to natively read [WARC, WET and WAT files](https://commoncrawl.org/blog/web-archiving-file-formats-explained).

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
 |-- warcContentType: string (nullable = false)
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
 |-- warcContentType: string (nullable = false)
 |-- warcHeaders: map (nullable = false)
 |    |-- key: string
 |    |-- value: array (valueContainsNull = false)
 |    |    |-- element: string (containsNull = false)
 |-- httpContentType: string (nullable = true)
 |-- httpHeaders: map (nullable = true)
 |    |-- key: string
 |    |-- value: array (valueContainsNull = false)
 |    |    |-- element: string (containsNull = false)
 |-- httpBody: string (nullable = true)
```

Predicate pushdown is supported for columns `warcId`, `warcType`, `warcTargetUri`, `warcDate`, `warcContentType` and `httpContentType`.

## Options

The WARC input format supports the following read options:

| Option name        | Type   | Description                                                                                                        |
|--------------------|--------|--------------------------------------------------------------------------------------------------------------------|
| lenient            | bool   | Enables lenient WARC parsing (e.g. allowing invalid UTF-8)                                                         |
| parseHTTP          | bool   | Parses the WARC body as a HTTP response. Replaces the `warcBody` column with `httpHeaders` and `httpBody` columns. |
| headersToLowerCase | bool   | Convert all WARC and HTTP header keys to lowercase.                                                                |
| filename           | bool   | Adds a `filename` column to the DataFrame.                                                                         |
| pathGlobFilter     | string | Read only files with file names matching the given glob pattern.                                                   |

## Examples

Below we give some examples on how to work with the WARC data source:

### Loading Dutch WARC response records

```scala
val nlResponseRecords = spark
  .read
  .format("org.rubigdata.warc")
  .load("/data/warc")
  .filter($"warcType" === "response")
  .filter($"warcTargetUri".contains(".nl"))
  .select($"warcTargetUri", $"warcBody")

nlResponseRecords.show()
```

This also nicely highlights the predicate pushdown and column pruning capabilities of the data source:

```scala
nlResponseRecords.explain()
```

Which will tell you:

```
== Physical Plan ==
*(1) Project [warcTargetUri#106, warcBody#110]
+- BatchScan warc [/data/warc][warcTargetUri#106, warcBody#110] warc [/data/warc]
     PushedFilters: [IsNotNull(warcTargetUri), EqualTo(warcType,response), StringContains(warcTargetUri,.nl)],
     ReadSchema: struct<warcTargetUri:string,warcBody:string> RuntimeFilters: []
```

### Extracting all titles from the HTML records using Jsoup

```scala
import org.jsoup.Jsoup

val extractTitle = udf( (html: String) => {
  val d = Jsoup.parse(html)
  d.title()
} )

val htmlTitles = spark
  .read
  .format("org.rubigdata.warc")
  .option("parseHTTP", true)
  .load("/data/warc")
  .filter($"warcType" === "response")
  .select($"warcTargetUri", extractTitle($"httpBody").as("title"))

htmlTitles.show()
```

### Get all the text from the WET files

```scala
spark
  .read
  .format("org.rubigdata.warc")
  .load("/data/wet")
  .filter($"warcType" === "conversion")
  .select($"warcTargetUri", $"warcBody".as("text"))
```

### Extract all links on the page using the WAT files

Note that the WAT files contain quite complex, nested JSON data, so it can be tricky to work with that data in Spark.

```scala
// First we filter the WAT records so that we only keep the records
// associated with a WARC response record.
val wat = spark
  .read
  .format("org.rubigdata.warc")
  .load("/data/wat")
  .filter($"warcType" === "metadata")
  .select(
    $"warcTargetUri",
    get_json_object(
      $"warcBody",
      "$.Envelope.Payload-Metadata"
    ).as("payloadMetadata")
  )
  .filter(
    get_json_object(
      $"payloadMetadata",
      "$.Actual-Content-Type"
    ) === "application/http; msgtype=response"
  )
  .select(
    $"warcTargetUri",
    get_json_object(
      $"payloadMetadata",
      "$.HTTP-Response-Metadata.HTML-Metadata"
    ).as("metadata")
  )

// Spark needs a specific JSON example to detect the schema
val jsonExample = wat.as[(String, String)].first._2

// We convert the JSON column into Spark nested types
val metadata = wat
  .select(
    $"warcTargetUri",
    from_json($"metadata", schema_of_json(jsonExample)).as("metadata")
  )

// Now we can directly query the `metadata` column
metadata
  .select($"warcTargetUri", $"metadata.Links.url".as("links"))
  .show()
```
