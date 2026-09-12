# S3 ETag as a `_metadata` column — design

Date: 2026-09-12
Target: Apache Spark 4.1.2, Scala 2.13.17, Hadoop 3.4.2, Java 17
Package: `com.example.spark.etag`

## Goal

Expose, for every input file of Spark's built-in Parquet, ORC, CSV and JSON
file sources, the S3 object's ETag as `_metadata.etag` and its user-defined
object metadata as `_metadata.user_metadata` (a JSON object string; Spark's
`FileSourceMetadataAttribute.isSupportedType` forbids map-typed metadata
fields), without changing how users read data. The map is what makes rclone's source
modification time reachable: rclone stores it as `X-Amz-Meta-Mtime`, a
decimal string of Unix seconds with nanosecond precision, and S3A hands it
back under the key `mtime`. After installing the extension:

```sql
SELECT _metadata.file_path, _metadata.etag,
       timestamp_seconds(CAST(get_json_object(_metadata.user_metadata, '$.mtime') AS DOUBLE)) AS source_mtime,
       *
FROM parquet.`s3a://bucket/prefix/`
```

works, as does `spark.read.parquet(...).select("_metadata.etag")` and reads
of catalog tables whose location is on S3.

Non-goals for this version: streaming reads, Data Source V2 relations, vendor
filesystems that do not implement Hadoop's `EtagSource`, per-row lookups on
arbitrary paths.

## How Spark exposes file metadata (verified against branch-4.1)

* `FileFormat.metadataSchemaFields` lists the fields of the `_metadata` struct.
  A "constant" field is declared with `FileSourceConstantMetadataStructField`.
* `FileIndex.listFiles` returns `PartitionDirectory(values, files)` where each
  file is a `FileStatusWithMetadata(fileStatus, metadata: Map[String, Any])`.
* `PartitionedFileUtil` copies that map into
  `PartitionedFile.otherConstantMetadataColumnValues`.
* The default extractor fills a constant field by looking up its name in that
  map, returning null when absent (`FileFormat.getFileConstantMetadataColumnValue`).
* `FilePruningRunner` evaluates filters on constant metadata fields at
  planning time, but it builds the metadata row with
  `FileFormat.createMetadataInternalRow`, which asserts that every field is
  one of the built-in four. A filter on `etag` or `user_metadata` therefore
  must not reach the wrapped index; `EtagFileIndex` strips such filters and
  applies them itself through `EtagFilePruner` after attaching the values.
* `FileSourceMetadataAttribute.isSupportedType` admits only primitive,
  decimal, binary, string and calendar-interval types, enforced by both
  `FileSourceConstantMetadataStructField.apply` and `unapply`, so the user
  metadata block is exposed as a JSON string; `get_json_object` and
  `from_json(..., 'map<string,string>')` read keys from it in SQL, and a
  filter built that way references only the constant string column, so it
  still prunes files.
* Hadoop's S3A `getXAttrs(path)` issues one HEAD request and returns every
  object header as an xattr named `header.<name>` with a UTF-8 value. User
  metadata keys are already stripped of `x-amz-meta-` by the AWS SDK, so
  rclone's mtime arrives as `header.mtime`; standard headers share the prefix
  (`header.Content-Length`, `header.ETag`, `header.x-amz-storage-class`, ...)
  and are excluded by name (`HeaderProcessing.XA_STANDARD_HEADERS`).
* Rules injected with `SparkSessionExtensions.injectResolutionRule` run inside
  the analyzer's Resolution batch immediately after `FindDataSourceTable` and
  `ResolveSQLOnFile`, i.e. in the same iteration in which a `LogicalRelation`
  is created and before `ResolveReferences` runs on the next iteration.
* Hadoop's `S3AFileStatus` implements `org.apache.hadoop.fs.EtagSource`, and
  `listStatus` on an S3A directory returns statuses with the ETag populated.
  Spark's own listing wraps statuses into plain `LocatedFileStatus` and
  serialises them for parallel listing, so the ETag is lost there; the
  extension must list again itself.

## Components

All classes live in `com.example.spark.etag`, one class per file.

### `S3EtagExtension extends (SparkSessionExtensions => Unit)`

Calls `injectResolutionRule(session => new EtagMetadataRule(session))`.
Named in `spark.sql.extensions`.

### `EtagMetadataRule(session) extends Rule[LogicalPlan]`

`apply` returns the plan unchanged when `spark.sql.s3etag.enabled` is false.
Streaming relations (`LogicalRelation.isStreaming`) are never rewritten.
Otherwise it uses `plan.resolveOperatorsUp` to rewrite each
`LogicalRelation(hfs: HadoopFsRelation, ...)` for which all of the following
hold:

1. `hfs.location` is not already an `EtagFileIndex`.
2. `hfs.fileFormat.getClass` is exactly one of `ParquetFileFormat`,
   `OrcFileFormat`, `CSVFileFormat`, `JsonFileFormat` (exact class, not
   `isInstanceOf`, so other subclasses are left alone).
3. Every root path of `hfs.location` has a scheme listed in
   `spark.sql.s3etag.schemes`.

The rewrite builds `hfs.copy(location = new EtagFileIndex(hfs.location),
fileFormat = <matching Etag format>)(session)` and copies the relation with the
new `HadoopFsRelation`. If the relation's `output` already contains a metadata
attribute (matched with `FileSourceMetadataAttribute`), that attribute is
removed and `withMetadataColumns()` is called on the new relation so the
struct type includes `etag`. Tags are copied from the old relation.

### `EtagFileIndex(delegate: FileIndex, hadoopConf: Configuration, fetchUserMetadata: Boolean, ignoreMissingFiles: Boolean = false) extends FileIndex`

Delegates `rootPaths`, `inputFiles`, `sizeInBytes`, `partitionSchema`,
`metadataOpsTimeNs` and
`refresh` (also clears the cache).

`listFiles(partitionFilters, dataFilters)` calls the delegate, then for each
`PartitionDirectory` maps every file to a copy whose `metadata` map has
`"etag" -> value` added, where `value` is a `String` or `null`.

ETag lookup: files are grouped by `getPath.getParent`. For each parent, once per
index instance, the filesystem for that path (obtained with
`path.getFileSystem(hadoopConf)`, where `hadoopConf` is the session's
`sessionState.newHadoopConfWithOptions(relation.options)` captured at
construction, so per-read options such as an S3A endpoint apply) is asked for
`listStatus(parent)`. Each returned status that implements `EtagSource` with a
non-null etag contributes `status.getPath -> etag` to the per-directory map.
The per-directory maps are held in a `ConcurrentHashMap[Path, Map[Path,
String]]`. Directories are listed sequentially on the driver; with the stated
scale (< 10k files) this is a few hundred requests at most.

Failures from `listStatus` propagate unchanged. A filesystem whose statuses do
not implement `EtagSource` produces an empty per-directory map, so every file
gets `null`.

User metadata: when `fetchUserMetadata` is true, after the delegate has
listed, every file path not yet cached gets one `fs.getXAttrs(path)` call,
issued from a driver-side fixed pool of 32 threads that is created for the
call and shut down afterwards. Each xattr named `header.<name>` that is not
one of S3A's standard headers becomes an entry keyed by `<name>` lower-cased
with any leading `x-amz-meta-` removed. The entries are serialised by
`UserMetadataJson.encode` (Jackson, keys sorted) and cached as
`ConcurrentHashMap[Path, Option[String]]`, `None` for an object without user
metadata, cleared by `refresh()`. A filesystem that throws
`UnsupportedOperationException` from `getXAttrs` yields `null`; other failures
propagate. Every file gets `"user_metadata" -> json-or-null` in its metadata
map; when `fetchUserMetadata` is false the value is always `null` and no
request is made.

### `EtagFilePruner(dataFilters: Seq[Expression])`

Exposes `heldBackFilters`, the input filters that reference the `etag` or
`user_metadata` metadata attribute, and `prune(directory)`, which drops files
failing those filters whose references are all file-constant metadata
attributes other than `file_block_start` and `file_block_length`. Binding and
evaluation mirror Spark's `FilePruningRunner`, but the metadata row is built
with `FileFormat.updateMetadataInternalRow` over a `PartitionedFile`, which
looks custom fields up by name. A filter such as
`get_json_object(_metadata.user_metadata, '$.mtime') > '1694500000'`
therefore prunes at planning time.

### `EtagFileFormats`

Four classes:

```
class EtagParquetFileFormat extends ParquetFileFormat
class EtagOrcFileFormat     extends OrcFileFormat
class EtagCsvFileFormat     extends CSVFileFormat
class EtagJsonFileFormat    extends JsonFileFormat
```

Each overrides `metadataSchemaFields` to append `ETAG_FIELD` then
`USER_METADATA_FIELD`, where

```
ETAG_FIELD = FileSourceConstantMetadataStructField("etag", StringType, nullable = true)
USER_METADATA_FIELD = FileSourceConstantMetadataStructField("user_metadata", StringType, nullable = true)
```

Both are strings, so Spark's default name lookup fills them and batch reading
is unaffected. Each also overrides `equals` to
`other.getClass == getClass` and `hashCode` to `getClass.hashCode`, because
the built-in Parquet and ORC formats consider any subclass equal to
themselves.

Write paths are untouched: the subclasses inherit `prepareWrite`, so a rewritten
relation used as an insert target behaves exactly like the built-in format.

### Configuration keys

| key | default | meaning |
|---|---|---|
| `spark.sql.s3etag.enabled` | `true` | Turn the rewrite on or off per session. |
| `spark.sql.s3etag.schemes` | `s3a` | Comma-separated URI schemes whose relations are rewritten. |
| `spark.sql.s3etag.userMetadata.enabled` | `true` | Fetch `user_metadata` (one HEAD per file at planning time). When off, the field is always null. |

Read through `session.conf.get(key, default)` at rule time, so `SET` in SQL takes effect for later queries.

## Data flow

1. Analyzer creates a `LogicalRelation` over a `HadoopFsRelation`.
2. `EtagMetadataRule` swaps in `EtagFileIndex` and the matching Etag format.
3. `ResolveReferences` resolves `_metadata` from the relation's `metadataOutput`;
   the struct now includes `etag`. `AddMetadataColumns` adds the column to the
   relation output.
4. `FileSourceStrategy` calls `listFiles`; `EtagFileIndex` attaches etags and
   user metadata, then applies filters on those fields.
5. `FileSourceScanExec` builds `PartitionedFile`s carrying the map; on
   executors the default name lookup fills `_metadata.etag`.
6. Any filter on `_metadata.etag` or on `get_json_object(_metadata.user_metadata, ...)` is applied
   by `EtagFilePruner` during step 4, so non-matching files are never scanned.

## Error handling

* Listing errors are not caught; they surface as they do for Spark's own
  listing.
* Missing etag is `null`, never an exception; so is user metadata on a
  filesystem without xattr support.
* HEAD request failures other than `UnsupportedOperationException` propagate,
  wrapped in an `IOException` naming the path; a `FileNotFoundException` yields
  `null` instead when `spark.sql.files.ignoreMissingFiles` is true.
* The rule never throws; a relation that fails any precondition is returned
  unchanged.

## Known limitation

Wrapping hides the concrete index class. The optimizer rule
`PruneFileSourcePartitions` only matches a bare `CatalogFileIndex`, so for
partitioned catalog tables it no longer rewrites the relation to a pruned
index. Partition pruning still happens, because `FileSourceStrategy` passes
partition filters into `listFiles` and `CatalogFileIndex.listFiles` prunes
there; only optimizer statistics for such tables may be less precise.

## Testing

Unit tests with ScalaTest against a local `SparkSession` (master
`local[2]`), no network.

Test filesystem `EtagLocalFileSystem` (test sources only) extends Hadoop's
`RawLocalFileSystem`, registered under scheme `etagfs` through
`fs.etagfs.impl` in the session's Hadoop configuration. Its `listStatus` and
`getFileStatus` wrap each file status in a subclass
that implements `EtagSource`, returning the hex MD5 of the file's bytes
(what S3 returns for single-part uploads). Directories return no etag.
It also overrides `getXAttrs` to mimic S3A: standard headers
`header.Content-Length` and `header.ETag` for every file, plus for files not
named `nometa*` the user metadata `header.mtime` (rclone's format, from the
file's modification time) and `header.x-amz-meta-name` (the file name, to
prove prefix stripping and to give a unique key for pruning tests).
Tests set `spark.sql.s3etag.schemes` to `etagfs`.

Cases:

1. `spark.read.parquet("etagfs://...")` selecting `_metadata.etag` matches
   MD5 computed independently for each file.
2. `SELECT _metadata.etag FROM parquet.\`etagfs://...\``.
3. `CREATE TABLE t USING parquet LOCATION 'etagfs://...'` then
   `SELECT _metadata.etag FROM t`.
4. Same as case 1 for ORC, CSV and JSON.
5. `WHERE _metadata.etag = <one file's etag>` returns only that file's rows and
   the physical plan's `FileSourceScanExec` reports a single file.
6. Partitioned directory: etags are correct and partition columns still
   resolve.
7. Reading a plain `file://` path yields no `etag` field (relation not
   rewritten), so `_metadata` keeps its default shape.
8. With `spark.sql.s3etag.schemes=file`, a local read yields an `etag` field
   whose value is `null` (no `EtagSource`).
9. `spark.sql.s3etag.enabled=false` disables the rewrite.
10. `EtagFileIndex` lists each directory once across repeated `listFiles`
    calls, and again after `refresh()` (counted through a test filesystem
    counter).

11. `from_json(_metadata.user_metadata, 'map<string,string>')['mtime']` equals
    the rclone-style mtime of each file, for all four formats.
12. A key absent from the user metadata yields `null`.
13. An object without user metadata yields `null` while others do not.
14. `WHERE from_json(...)['name'] = <one file's name>` scans one file.
15. `spark.sql.s3etag.userMetadata.enabled=false` leaves the field null and
    issues no `getXAttrs` call.
16. SQL `timestamp_seconds(CAST(coalesce(get_json_object(..., '$.x-mtime'),
    get_json_object(..., '$.mtime')) AS DOUBLE))` yields the file's
    modification second.
17. `EtagFileIndex` fetches user metadata once per file, again after
    `refresh()`, not at all when disabled, strips `header.` and
    `x-amz-meta-`, and excludes standard headers.

Manual verification against a real bucket is documented in the README:
upload a small file with `aws s3 cp`, compare `_metadata.etag` with
`aws s3api head-object --query ETag`.

## Build and layout

```
spark-s3-etag/
  build.sbt                      scala 2.13.17; spark-sql 4.1.2 % Provided;
                                 scalatest % Test
                                 (no hadoop-aws: only hadoop-common's EtagSource
                                 is needed, and it comes with spark-sql)
  project/build.properties       sbt.version=1.11.0
  src/main/scala/com/example/spark/etag/
    S3EtagExtension.scala
    EtagMetadataRule.scala
    EtagFileIndex.scala
    EtagFileFormats.scala
    EtagFilePruner.scala
    UserMetadataJson.scala
  src/test/scala/com/example/spark/etag/
    EtagLocalFileSystem.scala
    EtagLocalFileSystemSuite.scala
    EtagSparkSession.scala
    EtagMetadataColumnSuite.scala
    EtagMetadataRuleSuite.scala
    EtagFileIndexSuite.scala
    EtagFileFormatsSuite.scala
    EtagFilePrunerSuite.scala
    UserMetadataJsonSuite.scala
  README.md
```

`sbt package` produces `target/scala-2.13/spark-s3-etag_2.13-0.1.0.jar`.
No shading is needed because every dependency is provided by Spark.
