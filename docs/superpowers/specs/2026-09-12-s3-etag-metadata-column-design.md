# S3 ETag as a `_metadata` column — design

Date: 2026-09-12
Target: Apache Spark 4.1.2, Scala 2.13.17, Hadoop 3.4.2, Java 17
Package: `com.example.spark.etag`

## Goal

Expose the S3 object ETag of every input file as `_metadata.etag` for Spark's
built-in Parquet, ORC, CSV and JSON file sources, without changing how users
read data. After installing the extension:

```sql
SELECT _metadata.file_path, _metadata.etag, *
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
  planning time, so files can be pruned by ETag.
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

### `EtagFileIndex(delegate: FileIndex) extends FileIndex`

Delegates `rootPaths`, `inputFiles`, `sizeInBytes`, `partitionSchema`,
`metadataOpsTimeNs`, `partitionSpec` (if present on the delegate) and
`refresh` (also clears the cache).

`listFiles(partitionFilters, dataFilters)` calls the delegate, then for each
`PartitionDirectory` maps every file to a copy whose `metadata` map has
`"etag" -> value` added, where `value` is a `String` or `null`.

ETag lookup: files are grouped by `getPath.getParent`. For each parent, once per
index instance, the filesystem for that path (obtained with
`path.getFileSystem(hadoopConf)`, where `hadoopConf` is the session's
`sessionState.newHadoopConf()` captured at construction) is asked for
`listStatus(parent)`. Each returned status that implements `EtagSource` with a
non-null etag contributes `status.getPath -> etag` to the per-directory map.
The per-directory maps are held in a `ConcurrentHashMap[Path, Map[Path,
String]]`. Directories are listed sequentially on the driver; with the stated
scale (< 10k files) this is a few hundred requests at most.

Failures from `listStatus` propagate unchanged. A filesystem whose statuses do
not implement `EtagSource` produces an empty per-directory map, so every file
gets `null`.

### `EtagFileFormats`

Four classes:

```
class EtagParquetFileFormat extends ParquetFileFormat
class EtagOrcFileFormat     extends OrcFileFormat
class EtagCsvFileFormat     extends CSVFileFormat
class EtagJsonFileFormat    extends JsonFileFormat
```

Each overrides

```
override def metadataSchemaFields: Seq[StructField] =
  super.metadataSchemaFields :+ EtagFileFormats.ETAG_FIELD
```

with `ETAG_FIELD = FileSourceConstantMetadataStructField("etag", StringType,
nullable = true)`. Each also overrides `equals` to `other.getClass == getClass`
and `hashCode` to `getClass.hashCode`, because the built-in Parquet and ORC
formats consider any subclass equal to themselves.

Write paths are untouched: the subclasses inherit `prepareWrite`, so a rewritten
relation used as an insert target behaves exactly like the built-in format.

### Configuration keys

| key | default | meaning |
|---|---|---|
| `spark.sql.s3etag.enabled` | `true` | Turn the rewrite on or off per session. |
| `spark.sql.s3etag.schemes` | `s3a` | Comma-separated URI schemes whose relations are rewritten. |

Read through `session.conf.get(key, default)` at rule time, so `SET` in SQL takes effect for later queries.

## Data flow

1. Analyzer creates a `LogicalRelation` over a `HadoopFsRelation`.
2. `EtagMetadataRule` swaps in `EtagFileIndex` and the matching Etag format.
3. `ResolveReferences` resolves `_metadata` from the relation's `metadataOutput`;
   the struct now includes `etag`. `AddMetadataColumns` adds the column to the
   relation output.
4. `FileSourceStrategy` calls `listFiles`; `EtagFileIndex` attaches etags.
5. `FileSourceScanExec` builds `PartitionedFile`s carrying the map; on
   executors the default name lookup fills `_metadata.etag`.
6. Any filter on `_metadata.etag` is applied by `FilePruningRunner` during
   step 4, so non-matching files are never scanned.

## Error handling

* Listing errors are not caught; they surface as they do for Spark's own
  listing.
* Missing etag is `null`, never an exception.
* The rule never throws; a relation that fails any precondition is returned
  unchanged.

## Testing

Unit tests with ScalaTest against a local `SparkSession` (master
`local[2]`), no network.

Test filesystem `EtagLocalFileSystem` (test sources only) extends Hadoop's
`RawLocalFileSystem`, registered under scheme `etagfs` through
`fs.etagfs.impl` in the session's Hadoop configuration. Its `listStatus`,
`getFileStatus` and `listLocatedStatus` wrap each file status in a subclass
that implements `EtagSource`, returning the hex MD5 of the file's bytes
(what S3 returns for single-part uploads). Directories return no etag.
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

Manual verification against a real bucket is documented in the README:
upload a small file with `aws s3 cp`, compare `_metadata.etag` with
`aws s3api head-object --query ETag`.

## Build and layout

```
spark-s3-etag/
  build.sbt                      scala 2.13.17; spark-sql 4.1.2 % Provided;
                                 hadoop-aws 3.4.2 % Provided; scalatest % Test
  project/build.properties       sbt.version=1.11.x
  src/main/scala/com/example/spark/etag/
    S3EtagExtension.scala
    EtagMetadataRule.scala
    EtagFileIndex.scala
    EtagFileFormats.scala
  src/test/scala/com/example/spark/etag/
    EtagLocalFileSystem.scala
    EtagMetadataColumnSuite.scala
    EtagFileIndexSuite.scala
  README.md
```

`sbt package` produces `target/scala-2.13/spark-s3-etag_2.13-0.1.0.jar`.
No shading is needed because every dependency is provided by Spark.
