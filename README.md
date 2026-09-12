# spark-s3-etag

A Spark 4.1 extension that exposes, for every input file of a Parquet, ORC,
CSV or JSON read, the S3 object's ETag and its user-defined metadata as
`_metadata.etag` and `_metadata.user_metadata`.

```sql
SELECT _metadata.file_path,
       _metadata.etag,
       get_json_object(_metadata.user_metadata, '$.mtime') AS source_mtime,
       *
FROM parquet.`s3a://my-bucket/events/`
```

```python
(spark.read.parquet("s3a://my-bucket/events/")
      .select("_metadata.file_name", "_metadata.etag", "_metadata.user_metadata"))
```

`user_metadata` is a JSON object string such as
`{"mtime":"1694500000.123456789","uploader":"rclone"}`, with keys as S3
stores them (without the `x-amz-meta-` prefix), or `NULL` when the object has
no user metadata. Read it with `get_json_object(..., '$.key')` or turn it into
a map with `from_json(_metadata.user_metadata, 'map<string,string>')`. It is a
string rather than a map because Spark only allows primitive and string types
in the `_metadata` struct.

Filters on either field, such as `WHERE _metadata.etag = '...'` or
`WHERE get_json_object(_metadata.user_metadata, '$.mtime') > '1694500000'`,
are applied while listing, so non-matching files are never opened.

## The rclone `mtime` case

rclone stores the source file's modification time as user metadata
`X-Amz-Meta-Mtime`, a decimal string of Unix seconds with nanoseconds, for
example `1694500000.123456789`. It arrives under the key `mtime`:

```sql
SELECT _metadata.file_name,
       timestamp_seconds(CAST(coalesce(
         get_json_object(_metadata.user_metadata, '$.mtime'),
         get_json_object(_metadata.user_metadata, '$.x-mtime')) AS DOUBLE)) AS source_modified_at
FROM parquet.`s3a://my-bucket/events/`
```

A missing key yields `NULL`, so `coalesce` across keys works as expected.

## Requirements

* Apache Spark 4.1.x on Java 17+, with the `hadoop-aws` module on the
  classpath (that is what provides `s3a://`).
* Scala 2.13 (Spark 4 only ships for 2.13).

## Build

```bash
brew install sbt      # once
sbt package           # produces target/scala-2.13/spark-s3-etag_2.13-0.1.0.jar
sbt test              # runs the offline test suite
```

## Install

Add the jar and name the extension class:

```bash
spark-submit \
  --jars spark-s3-etag_2.13-0.1.0.jar \
  --conf spark.sql.extensions=com.example.spark.etag.S3EtagExtension \
  your_job.py
```

or in a `SparkSession.builder`:

```python
spark = (SparkSession.builder
         .config("spark.jars", "spark-s3-etag_2.13-0.1.0.jar")
         .config("spark.sql.extensions", "com.example.spark.etag.S3EtagExtension")
         .getOrCreate())
```

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `spark.sql.s3etag.enabled` | `true` | Turn the rewrite on or off for the session. |
| `spark.sql.s3etag.schemes` | `s3a` | Comma-separated URI schemes whose reads get the new fields. |
| `spark.sql.s3etag.userMetadata.enabled` | `true` | Fetch `user_metadata`. Costs one HEAD request per file at planning time; when off, the field is always `NULL` and no request is made. |

All three can be changed at runtime with `SET` and take effect for later
queries.

## Cost

* `etag` comes from the directory listing: one S3 LIST request per directory,
  the same order of cost as Spark's own listing.
* `user_metadata` needs one S3 HEAD request per file, issued from the driver
  32 at a time while the query is planned and cached for the life of the
  plan. For a scan over many thousands of files, turn it off unless you use it.

## How it works

1. An analyzer rule rewrites each file relation on an enabled scheme: the file
   index is wrapped, and the file format is swapped for a subclass that adds
   `etag` and `user_metadata` to the `_metadata` struct.
2. While Spark lists files for a query, the wrapper issues one `listStatus`
   per directory and reads the ETag from Hadoop's `EtagSource` statuses, then
   one `getXAttrs` per file, which S3A answers with the object's headers.
   Every `header.*` entry that is not a standard HTTP or `x-amz-*` system
   header is user metadata.
3. Filters on `_metadata.etag` and `_metadata.user_metadata` are applied by
   the wrapper during listing.

## Checking against a real bucket

```bash
aws s3 cp hello.csv s3://my-bucket/check/hello.csv --metadata mtime=1694500000.123456789
aws s3api head-object --bucket my-bucket --key check/hello.csv --query '[ETag, Metadata]'
```

```python
spark.read.csv("s3a://my-bucket/check/").select("_metadata.etag", "_metadata.user_metadata").show(truncate=False)
```

The ETag matches minus the surrounding quotes S3 prints, and the JSON shows
`{"mtime":"1694500000.123456789"}`.

## Limitations

* Objects uploaded in several parts have an ETag that is not the MD5 of the
  object. The column reports whatever S3 returns.
* Only Data Source V1 file relations are rewritten. The defaults in Spark 4.1
  route Parquet, ORC, CSV and JSON through V1.
* Streaming reads are not rewritten.
* Filesystems whose file statuses do not implement `EtagSource` yield a null
  `etag`; filesystems without `getXAttrs` support yield a null `user_metadata`
  (for example plain `file://`, or vendor S3 clients).
* Wrapping the file index hides its concrete class from Spark's
  `PruneFileSourcePartitions` optimizer rule. Partition pruning still happens
  during listing, but optimizer statistics for partitioned catalog tables can
  be less precise.
