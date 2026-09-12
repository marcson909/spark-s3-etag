# S3 ETag Metadata Column Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Spark 4.1 extension jar that makes `_metadata.etag` available on every Parquet, ORC, CSV and JSON read from S3A paths.

**Architecture:** A `SparkSessionExtensions` resolution rule rewrites each file relation on an enabled URI scheme: the `FileIndex` is wrapped so listing attaches each file's ETag (one Hadoop `listStatus` per directory, cached), and the `FileFormat` is swapped for a subclass that adds an `etag` field to the `_metadata` struct. Because Spark's built-in file pruner asserts on unknown metadata fields, the wrapper keeps `etag` filters away from the inner index and applies them itself.

**Tech Stack:** Scala 2.13.17, sbt 1.11.0, Apache Spark 4.1.2 (`spark-sql`, provided), Hadoop 3.4.2 (comes with Spark), ScalaTest 3.2.19, Java 17.

## Global Constraints

- Spark version: `4.1.2`; Scala: `2.13.17`; Java: `17`. Never add a Spark or Hadoop dependency with any other version.
- Package for all production and test classes: `com.example.spark.etag`.
- The only runtime dependency is Spark itself. `spark-sql` is `Provided`; the jar produced by `sbt package` must contain only this project's classes (no shading, no assembly).
- Metadata field name is exactly `etag`, type string, nullable.
- Config keys: `spark.sql.s3etag.enabled` (default `true`) and `spark.sql.s3etag.schemes` (default `s3a`).
- Tests must run offline. The `etagfs` test filesystem stands in for S3A.
- Every test class extends `org.scalatest.funsuite.AnyFunSuite`.
- Commit after every task with the attribution trailer `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Spec: `docs/superpowers/specs/2026-09-12-s3-etag-metadata-column-design.md`.

## File structure

| Path | Responsibility |
|---|---|
| `build.sbt`, `project/build.properties`, `.gitignore` | Build definition. |
| `src/main/scala/com/example/spark/etag/EtagFileFormats.scala` | The `etag` struct field, the four format subclasses, and `replacementFor`. |
| `src/main/scala/com/example/spark/etag/EtagFilePruner.scala` | Applies filters that mention `etag` to a listed directory. |
| `src/main/scala/com/example/spark/etag/EtagFileIndex.scala` | Wraps a `FileIndex`; attaches etags; caches per directory; prunes. |
| `src/main/scala/com/example/spark/etag/EtagMetadataRule.scala` | Analyzer rule that rewrites eligible relations. |
| `src/main/scala/com/example/spark/etag/S3EtagExtension.scala` | Entry point named in `spark.sql.extensions`. |
| `src/test/scala/com/example/spark/etag/EtagLocalFileSystem.scala` | Test filesystem (`etagfs://`) whose statuses carry MD5 etags. |
| `src/test/scala/com/example/spark/etag/EtagSparkSession.scala` | Shared local `SparkSession` for end-to-end tests. |
| `src/test/scala/com/example/spark/etag/*Suite.scala` | One suite per production file plus the end-to-end suite. |
| `README.md` | Build, install, usage, config, limitations, manual S3 check. |

---

### Task 1: Project scaffold and `EtagFileFormats`

**Files:**
- Create: `build.sbt`
- Create: `project/build.properties`
- Create: `.gitignore`
- Create: `src/main/scala/com/example/spark/etag/EtagFileFormats.scala`
- Test: `src/test/scala/com/example/spark/etag/EtagFileFormatsSuite.scala`

**Interfaces:**
- Consumes: Spark's `ParquetFileFormat`, `OrcFileFormat`, `CSVFileFormat`, `JsonFileFormat`, `FileSourceConstantMetadataStructField`.
- Produces:
  - `object EtagFileFormats { val ETAG_FIELD_NAME: String = "etag"; val ETAG_FIELD: StructField; def replacementFor(format: FileFormat): Option[FileFormat] }`
  - `class EtagParquetFileFormat`, `class EtagOrcFileFormat`, `class EtagCsvFileFormat`, `class EtagJsonFileFormat`, each a subclass of the matching Spark format.

- [ ] **Step 1: Install sbt (skip if `sbt --version` already works)**

Run:
```bash
brew install sbt
```
Then confirm:
```bash
sbt --version
```
Expected: prints an sbt runner/launcher version. The project pins its own sbt version in `project/build.properties`, so any launcher is fine.

- [ ] **Step 2: Write the build files**

`project/build.properties`:
```
sbt.version=1.11.0
```

`build.sbt`:
```scala
ThisBuild / organization := "com.example"
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.13.17"

val sparkVersion = "4.1.2"

lazy val root = (project in file("."))
  .settings(
    name := "spark-s3-etag",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-release", "17"),
    // Spark 4 on Java 17 needs these module opens. spark-submit adds them itself;
    // forked test JVMs must add them explicitly.
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq(
      "-Xmx2g",
      "-Djdk.reflect.useDirectMethodHandle=false",
      "--enable-native-access=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    )
  )
```

`.gitignore`:
```
target/
project/target/
project/project/
.bsp/
.idea/
.metals/
.bloop/
spark-warehouse/
metastore_db/
derby.log
```

- [ ] **Step 3: Write the failing test**

`src/test/scala/com/example/spark/etag/EtagFileFormatsSuite.scala`:
```scala
package com.example.spark.etag

import org.apache.spark.sql.catalyst.expressions.FileSourceConstantMetadataStructField
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.types.StringType
import org.scalatest.funsuite.AnyFunSuite

class EtagFileFormatsSuite extends AnyFunSuite {

  private val etagFormats = Seq(
    new EtagParquetFileFormat,
    new EtagOrcFileFormat,
    new EtagCsvFileFormat,
    new EtagJsonFileFormat)

  test("every etag format appends a nullable string constant field named etag") {
    etagFormats.foreach { format =>
      val fields = format.metadataSchemaFields
      val last = fields.last
      assert(last.name == "etag", s"${format.getClass.getSimpleName} last field")
      assert(last.dataType == StringType)
      assert(last.nullable)
      assert(FileSourceConstantMetadataStructField.unapply(last).isDefined,
        "etag must be a file-constant metadata field")
      assert(fields.map(_.name).contains(FileFormat.FILE_PATH), "built-in fields must be kept")
    }
  }

  test("parquet keeps its generated row_index field ahead of etag") {
    val names = new EtagParquetFileFormat.metadataSchemaFields.map(_.name)
    assert(names.indexOf(ParquetFileFormat.ROW_INDEX) < names.indexOf("etag"))
  }

  test("replacementFor maps each built-in format to its etag subclass") {
    assert(EtagFileFormats.replacementFor(new ParquetFileFormat).exists(_.isInstanceOf[EtagParquetFileFormat]))
    assert(EtagFileFormats.replacementFor(new OrcFileFormat).exists(_.isInstanceOf[EtagOrcFileFormat]))
    assert(EtagFileFormats.replacementFor(new CSVFileFormat).exists(_.isInstanceOf[EtagCsvFileFormat]))
    assert(EtagFileFormats.replacementFor(new JsonFileFormat).exists(_.isInstanceOf[EtagJsonFileFormat]))
  }

  test("replacementFor ignores formats it does not know, including its own subclasses") {
    assert(EtagFileFormats.replacementFor(new TextFileFormat).isEmpty)
    etagFormats.foreach(f => assert(EtagFileFormats.replacementFor(f).isEmpty))
  }

  test("etag formats compare equal only to the same class") {
    assert(new EtagParquetFileFormat == new EtagParquetFileFormat)
    assert(new EtagParquetFileFormat != new ParquetFileFormat)
    assert(new EtagOrcFileFormat != new OrcFileFormat)
    assert(new EtagParquetFileFormat.hashCode == new EtagParquetFileFormat.hashCode)
  }
}
```

- [ ] **Step 4: Run the test to verify it fails to compile**

Run:
```bash
sbt test
```
Expected: compilation error, `not found: type EtagParquetFileFormat` (or similar). The first run downloads Spark and can take several minutes.

- [ ] **Step 5: Write the implementation**

`src/main/scala/com/example/spark/etag/EtagFileFormats.scala`:
```scala
package com.example.spark.etag

import org.apache.spark.sql.catalyst.expressions.FileSourceConstantMetadataStructField
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{StringType, StructField}

/**
 * The `_metadata.etag` field and the file formats that expose it.
 *
 * Spark builds the `_metadata` struct from `FileFormat.metadataSchemaFields`. A "constant" field
 * is filled per file by looking its name up in the metadata map that the FileIndex attached to the
 * file (see EtagFileIndex), so the formats below only need to declare the field.
 */
object EtagFileFormats {
  val ETAG_FIELD_NAME: String = "etag"

  /** Nullable because not every filesystem, and not every S3 object listing, provides an ETag. */
  val ETAG_FIELD: StructField =
    FileSourceConstantMetadataStructField(ETAG_FIELD_NAME, StringType, nullable = true)

  /**
   * The etag-aware replacement for one of Spark's built-in file formats, or None when the format
   * is anything else. Matches on the exact class so that other subclasses, including the ones
   * defined here, are left alone.
   */
  def replacementFor(format: FileFormat): Option[FileFormat] = {
    val formatClass = format.getClass
    if (formatClass == classOf[ParquetFileFormat]) Some(new EtagParquetFileFormat)
    else if (formatClass == classOf[OrcFileFormat]) Some(new EtagOrcFileFormat)
    else if (formatClass == classOf[CSVFileFormat]) Some(new EtagCsvFileFormat)
    else if (formatClass == classOf[JsonFileFormat]) Some(new EtagJsonFileFormat)
    else None
  }

  private[etag] def withEtag(fields: Seq[StructField]): Seq[StructField] = fields :+ ETAG_FIELD
}

// Each subclass overrides equals/hashCode to compare by exact class. The built-in Parquet and
// ORC formats treat any subclass as equal to themselves, which would let Spark's plan cache
// mistake a rewritten relation for the original.

class EtagParquetFileFormat extends ParquetFileFormat {
  override def metadataSchemaFields: Seq[StructField] =
    EtagFileFormats.withEtag(super.metadataSchemaFields)
  override def equals(other: Any): Boolean = other != null && other.getClass == getClass
  override def hashCode(): Int = getClass.hashCode()
}

class EtagOrcFileFormat extends OrcFileFormat {
  override def metadataSchemaFields: Seq[StructField] =
    EtagFileFormats.withEtag(super.metadataSchemaFields)
  override def equals(other: Any): Boolean = other != null && other.getClass == getClass
  override def hashCode(): Int = getClass.hashCode()
}

class EtagCsvFileFormat extends CSVFileFormat {
  override def metadataSchemaFields: Seq[StructField] =
    EtagFileFormats.withEtag(super.metadataSchemaFields)
  override def equals(other: Any): Boolean = other != null && other.getClass == getClass
  override def hashCode(): Int = getClass.hashCode()
}

class EtagJsonFileFormat extends JsonFileFormat {
  override def metadataSchemaFields: Seq[StructField] =
    EtagFileFormats.withEtag(super.metadataSchemaFields)
  override def equals(other: Any): Boolean = other != null && other.getClass == getClass
  override def hashCode(): Int = getClass.hashCode()
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run:
```bash
sbt test
```
Expected: `EtagFileFormatsSuite` reports 5 tests passed, `All tests passed.`

- [ ] **Step 7: Commit**

```bash
git add build.sbt project/build.properties .gitignore src
git commit -m "Add build and etag-aware file formats

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: `etagfs` test filesystem

**Files:**
- Create: `src/test/scala/com/example/spark/etag/EtagLocalFileSystem.scala`
- Test: `src/test/scala/com/example/spark/etag/EtagLocalFileSystemSuite.scala`

**Interfaces:**
- Consumes: Hadoop `RawLocalFileSystem`, `FileStatus`, `EtagSource`.
- Produces (test scope only):
  - `class EtagLocalFileSystem extends RawLocalFileSystem` registered by setting Hadoop conf `fs.etagfs.impl` to its class name; URIs look like `etagfs:///absolute/local/path`.
  - `object EtagLocalFileSystem { val Scheme = "etagfs"; val listStatusCalls: AtomicInteger; def md5Hex(file: java.io.File): String; def md5Hex(bytes: Array[Byte]): String }`
  - `class EtagFileStatus(other: FileStatus, etag: String) extends FileStatus with EtagSource`

- [ ] **Step 1: Write the failing test**

`src/test/scala/com/example/spark/etag/EtagLocalFileSystemSuite.scala`:
```scala
package com.example.spark.etag

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{EtagSource, Path}
import org.scalatest.funsuite.AnyFunSuite

class EtagLocalFileSystemSuite extends AnyFunSuite {

  private val abcMd5 = "900150983cd24fb0d6963f7d28e17f72" // RFC 1321 test vector for "abc"

  test("md5Hex matches the RFC 1321 test vector") {
    assert(EtagLocalFileSystem.md5Hex("abc".getBytes(StandardCharsets.US_ASCII)) == abcMd5)
  }

  test("listStatus and getFileStatus return statuses carrying the MD5 as etag") {
    val dir = Files.createTempDirectory("etagfs-suite").toFile
    Files.write(new File(dir, "a.txt").toPath, "abc".getBytes(StandardCharsets.US_ASCII))
    val conf = new Configuration()
    conf.set("fs.etagfs.impl", classOf[EtagLocalFileSystem].getName)
    val dirPath = new Path("etagfs://" + dir.getAbsolutePath)
    val fs = dirPath.getFileSystem(conf)

    val listed = fs.listStatus(dirPath)
    assert(listed.length == 1)
    assert(listed.head.getPath.toUri.getScheme == "etagfs")
    assert(listed.head.asInstanceOf[EtagSource].getEtag == abcMd5)

    val single = fs.getFileStatus(new Path(dirPath, "a.txt"))
    assert(single.asInstanceOf[EtagSource].getEtag == abcMd5)

    assert(!fs.getFileStatus(dirPath).isInstanceOf[EtagSource], "directories carry no etag")
  }

  test("listStatusCalls counts every listStatus call") {
    val dir = Files.createTempDirectory("etagfs-count").toFile
    val conf = new Configuration()
    conf.set("fs.etagfs.impl", classOf[EtagLocalFileSystem].getName)
    val dirPath = new Path("etagfs://" + dir.getAbsolutePath)
    val fs = dirPath.getFileSystem(conf)
    val before = EtagLocalFileSystem.listStatusCalls.get()
    fs.listStatus(dirPath)
    fs.listStatus(dirPath)
    assert(EtagLocalFileSystem.listStatusCalls.get() == before + 2)
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
sbt "testOnly com.example.spark.etag.EtagLocalFileSystemSuite"
```
Expected: compilation error, `not found: value EtagLocalFileSystem`.

- [ ] **Step 3: Write the test filesystem**

`src/test/scala/com/example/spark/etag/EtagLocalFileSystem.scala`:
```scala
package com.example.spark.etag

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.{EtagSource, FileStatus, Path, RawLocalFileSystem}

/** A file status that also carries an ETag, the way Hadoop's S3AFileStatus does. */
class EtagFileStatus(other: FileStatus, etag: String) extends FileStatus(other) with EtagSource {
  override def getEtag: String = etag
}

/**
 * Local filesystem registered under the `etagfs` scheme whose file statuses carry an ETag equal
 * to the hex MD5 of the file, which is what S3 returns for objects uploaded in a single part.
 *
 * Register it with Hadoop conf `fs.etagfs.impl = com.example.spark.etag.EtagLocalFileSystem`
 * and address local files as `etagfs:///absolute/path`.
 */
class EtagLocalFileSystem extends RawLocalFileSystem {

  override def getScheme: String = EtagLocalFileSystem.Scheme

  // RawLocalFileSystem reports file:/// here; returning our own URI makes every path this
  // filesystem hands out keep the etagfs scheme, so Spark routes later calls back to us.
  override def getUri: URI = EtagLocalFileSystem.Uri

  override def getFileStatus(path: Path): FileStatus = withEtag(super.getFileStatus(path))

  override def listStatus(path: Path): Array[FileStatus] = {
    EtagLocalFileSystem.listStatusCalls.incrementAndGet()
    super.listStatus(path).map(withEtag)
  }

  private def withEtag(status: FileStatus): FileStatus =
    if (status.isDirectory) status
    else new EtagFileStatus(status, EtagLocalFileSystem.md5Hex(pathToFile(status.getPath)))
}

object EtagLocalFileSystem {
  val Scheme: String = "etagfs"
  val Uri: URI = URI.create("etagfs:///")

  /** Total number of listStatus calls across all instances, for cache tests. */
  val listStatusCalls: AtomicInteger = new AtomicInteger(0)

  def md5Hex(file: File): String = md5Hex(Files.readAllBytes(file.toPath))

  def md5Hex(bytes: Array[Byte]): String =
    MessageDigest.getInstance("MD5").digest(bytes).map(b => f"${b & 0xff}%02x").mkString
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:
```bash
sbt "testOnly com.example.spark.etag.EtagLocalFileSystemSuite"
```
Expected: 3 tests passed.

- [ ] **Step 5: Commit**

```bash
git add src/test
git commit -m "Add etagfs test filesystem with MD5 etags

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: `EtagFilePruner`

**Files:**
- Create: `src/main/scala/com/example/spark/etag/EtagFilePruner.scala`
- Test: `src/test/scala/com/example/spark/etag/EtagFilePrunerSuite.scala`

**Interfaces:**
- Consumes: `EtagFileFormats.ETAG_FIELD_NAME` (Task 1); Spark's `FileSourceConstantMetadataAttribute`, `PartitionedFileUtil.getPartitionedFile`, `FileFormat.updateMetadataInternalRow`, `FileFormat.BASE_METADATA_EXTRACTORS`, `Predicate.createInterpreted`.
- Produces: `class EtagFilePruner(dataFilters: Seq[Expression]) { val etagFilters: Seq[Expression]; def prune(directory: PartitionDirectory): PartitionDirectory }`.
  - `etagFilters`: every input filter that references the `etag` metadata attribute. The caller must not pass these to the wrapped FileIndex.
  - `prune`: drops files that fail the etag filters whose references are all file-constant metadata attributes (excluding `file_block_start`/`file_block_length`). Files must already carry `"etag"` in their metadata map.

- [ ] **Step 1: Write the failing test**

`src/test/scala/com/example/spark/etag/EtagFilePrunerSuite.scala`:
```scala
package com.example.spark.etag

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, GreaterThan, Literal, Or}
import org.apache.spark.sql.execution.datasources.{FileFormat, FileStatusWithMetadata, PartitionDirectory}
import org.apache.spark.sql.types.{LongType, StructField}
import org.scalatest.funsuite.AnyFunSuite

class EtagFilePrunerSuite extends AnyFunSuite {

  private def metadataAttribute(field: StructField): AttributeReference =
    AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()

  private val etag = metadataAttribute(EtagFileFormats.ETAG_FIELD)
  private val fileName = metadataAttribute(
    FileFormat.BASE_METADATA_FIELDS.find(_.name == FileFormat.FILE_NAME).get)
  private val blockStart = metadataAttribute(
    FileFormat.BASE_METADATA_FIELDS.find(_.name == FileFormat.FILE_BLOCK_START).get)
  private val dataColumn = AttributeReference("value", LongType)()

  /** A directory of files named by their etag, e.g. ("a.parquet", "etag-a"); null means no etag. */
  private def directory(files: (String, String)*): PartitionDirectory =
    PartitionDirectory(InternalRow.empty, files.map { case (name, fileEtag) =>
      val status = new FileStatus(10L, false, 1, 10L, 0L, new Path("etagfs:///data/" + name))
      FileStatusWithMetadata(status, Map(EtagFileFormats.ETAG_FIELD_NAME -> fileEtag))
    })

  private def names(directory: PartitionDirectory): Seq[String] =
    directory.files.map(_.getPath.getName)

  private val threeFiles = directory("a" -> "etag-a", "b" -> "etag-b", "c" -> null)

  test("etag equality keeps only the matching file") {
    val pruner = new EtagFilePruner(Seq(EqualTo(etag, Literal("etag-b"))))
    assert(pruner.etagFilters.size == 1)
    assert(names(pruner.prune(threeFiles)) == Seq("b"))
  }

  test("a file with a null etag never matches an equality filter") {
    val pruner = new EtagFilePruner(Seq(EqualTo(etag, Literal("anything"))))
    assert(names(pruner.prune(threeFiles)).isEmpty)
  }

  test("etag combined with another constant metadata column prunes on both") {
    val filter = And(EqualTo(etag, Literal("etag-a")), EqualTo(fileName, Literal("a")))
    assert(names(new EtagFilePruner(Seq(filter)).prune(threeFiles)) == Seq("a"))
    val mismatch = And(EqualTo(etag, Literal("etag-a")), EqualTo(fileName, Literal("b")))
    assert(names(new EtagFilePruner(Seq(mismatch)).prune(threeFiles)).isEmpty)
  }

  test("a filter mixing etag with a data column is reported but does not prune") {
    val filter = Or(EqualTo(etag, Literal("etag-a")), GreaterThan(dataColumn, Literal(1L)))
    val pruner = new EtagFilePruner(Seq(filter))
    assert(pruner.etagFilters == Seq(filter))
    assert(names(pruner.prune(threeFiles)) == Seq("a", "b", "c"))
  }

  test("a filter mixing etag with file_block_start is reported but does not prune") {
    val filter = And(EqualTo(etag, Literal("etag-a")), GreaterThan(blockStart, Literal(0L)))
    val pruner = new EtagFilePruner(Seq(filter))
    assert(pruner.etagFilters == Seq(filter))
    assert(names(pruner.prune(threeFiles)) == Seq("a", "b", "c"))
  }

  test("filters that do not mention etag are neither reported nor applied") {
    val pruner = new EtagFilePruner(Seq(EqualTo(fileName, Literal("zzz"))))
    assert(pruner.etagFilters.isEmpty)
    assert(names(pruner.prune(threeFiles)) == Seq("a", "b", "c"))
  }

  test("no filters means no pruning") {
    assert(names(new EtagFilePruner(Nil).prune(threeFiles)) == Seq("a", "b", "c"))
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
sbt "testOnly com.example.spark.etag.EtagFilePrunerSuite"
```
Expected: compilation error, `not found: type EtagFilePruner`.

- [ ] **Step 3: Write the implementation**

`src/main/scala/com/example/spark/etag/EtagFilePruner.scala`:
```scala
package com.example.spark.etag

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BoundReference, Expression, FileSourceConstantMetadataAttribute, GenericInternalRow, Predicate}
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionDirectory, PartitionedFileUtil}

/**
 * Prunes listed files with data filters that mention the `etag` metadata column.
 *
 * Spark's own FilePruningRunner handles filters on file-constant metadata columns, but it builds
 * the metadata row with FileFormat.createMetadataInternalRow, which only knows the built-in fields
 * and fails an assertion on `etag`. So EtagFileIndex keeps `etagFilters` away from the wrapped
 * index and calls `prune` here after the etags have been attached.
 *
 * As in Spark, only filters whose references are all file-constant metadata columns can prune,
 * and `file_block_start` / `file_block_length` are excluded because splits are not known yet.
 */
class EtagFilePruner(dataFilters: Seq[Expression]) {

  /** Every filter that references `etag`. None of these may reach the wrapped FileIndex. */
  val etagFilters: Seq[Expression] = dataFilters.filter(_.references.exists(isEtag))

  private val prunableFilter: Option[Expression] =
    etagFilters.filter(_.references.forall(isPrunableMetadata)).reduceOption(And)

  /** Metadata column names in the order the bound predicate expects them in the row. */
  private val requiredColumnNames = mutable.ArrayBuffer.empty[String]

  private val boundPredicate = prunableFilter.map { filter =>
    Predicate.createInterpreted(filter.transform {
      case attr: AttributeReference => BoundReference(indexOf(attr.name), attr.dataType, nullable = true)
    })
  }

  private def indexOf(columnName: String): Int = {
    val existing = requiredColumnNames.indexOf(columnName)
    if (existing >= 0) existing
    else {
      requiredColumnNames += columnName
      requiredColumnNames.length - 1
    }
  }

  def prune(directory: PartitionDirectory): PartitionDirectory = boundPredicate match {
    case None => directory
    case Some(predicate) =>
      val kept = directory.files.filter { file =>
        val partitionedFile =
          PartitionedFileUtil.getPartitionedFile(file, file.getPath, directory.values, 0L, file.getLen)
        val row = FileFormat.updateMetadataInternalRow(
          new GenericInternalRow(requiredColumnNames.length),
          requiredColumnNames.toSeq,
          partitionedFile,
          FileFormat.BASE_METADATA_EXTRACTORS)
        predicate.eval(row)
      }
      directory.copy(files = kept)
  }

  private def isEtag(attr: Attribute): Boolean = attr match {
    case FileSourceConstantMetadataAttribute(metadata) => metadata.name == EtagFileFormats.ETAG_FIELD_NAME
    case _ => false
  }

  private def isPrunableMetadata(attr: Attribute): Boolean = attr match {
    case FileSourceConstantMetadataAttribute(metadata) =>
      metadata.name != FileFormat.FILE_BLOCK_START && metadata.name != FileFormat.FILE_BLOCK_LENGTH
    case _ => false
  }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:
```bash
sbt "testOnly com.example.spark.etag.EtagFilePrunerSuite"
```
Expected: 7 tests passed.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "Add file pruning on the etag metadata column

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: `EtagFileIndex`

**Files:**
- Create: `src/main/scala/com/example/spark/etag/EtagFileIndex.scala`
- Test: `src/test/scala/com/example/spark/etag/EtagFileIndexSuite.scala`

**Interfaces:**
- Consumes: `EtagFilePruner` (Task 3), `EtagFileFormats.ETAG_FIELD_NAME` (Task 1), `EtagLocalFileSystem` (Task 2, tests only).
- Produces: `class EtagFileIndex(val delegate: FileIndex, hadoopConf: Configuration) extends FileIndex`. `listFiles` returns the delegate's directories with `"etag" -> String-or-null` added to every file's metadata map, pruned by etag filters. `refresh()` clears the etag cache and refreshes the delegate.

- [ ] **Step 1: Write the failing test**

`src/test/scala/com/example/spark/etag/EtagFileIndexSuite.scala`:
```scala
package com.example.spark.etag

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.execution.datasources.{FileIndex, FileStatusWithMetadata, PartitionDirectory}
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite

/** A FileIndex that returns a fixed listing and records the filters it was given. */
class FixedFileIndex(directories: Seq[PartitionDirectory]) extends FileIndex {
  var refreshCount = 0
  var lastDataFilters: Seq[Expression] = Nil
  override def rootPaths: Seq[Path] = directories.flatMap(_.files.map(_.getPath.getParent)).distinct
  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    lastDataFilters = dataFilters
    directories
  }
  override def inputFiles: Array[String] = directories.flatMap(_.files.map(_.getPath.toString)).toArray
  override def refresh(): Unit = refreshCount += 1
  override def sizeInBytes: Long = directories.flatMap(_.files.map(_.getLen)).sum
  override def partitionSchema: StructType = StructType(Nil)
}

class EtagFileIndexSuite extends AnyFunSuite {

  private val conf: Configuration = {
    val c = new Configuration()
    c.set("fs.etagfs.impl", classOf[EtagLocalFileSystem].getName)
    c
  }

  private def writeFile(dir: File, name: String, content: String): File = {
    val file = new File(dir, name)
    Files.write(file.toPath, content.getBytes(StandardCharsets.UTF_8))
    file
  }

  private def statusOf(path: Path): FileStatusWithMetadata =
    FileStatusWithMetadata(path.getFileSystem(conf).getFileStatus(path))

  private def etagsOf(directories: Seq[PartitionDirectory]): Map[String, Any] =
    directories.flatMap(_.files.map(f => f.getPath.getName -> f.metadata(EtagFileFormats.ETAG_FIELD_NAME))).toMap

  private val etagAttribute = AttributeReference(
    EtagFileFormats.ETAG_FIELD.name, EtagFileFormats.ETAG_FIELD.dataType,
    EtagFileFormats.ETAG_FIELD.nullable, EtagFileFormats.ETAG_FIELD.metadata)()

  /** Two etagfs directories with two and one files, and one plain local file. */
  private class Fixture {
    val dir1: File = Files.createTempDirectory("etag-index-1").toFile
    val dir2: File = Files.createTempDirectory("etag-index-2").toFile
    val a: File = writeFile(dir1, "a.txt", "aaa")
    val b: File = writeFile(dir1, "b.txt", "bbb")
    val c: File = writeFile(dir2, "c.txt", "ccc")
    val local: File = writeFile(dir2, "local.txt", "local")
    def etagfs(file: File): Path = new Path("etagfs://" + file.getAbsolutePath)
    val listing: Seq[PartitionDirectory] = Seq(
      PartitionDirectory(InternalRow.empty, Seq(statusOf(etagfs(a)), statusOf(etagfs(b)))),
      PartitionDirectory(InternalRow.empty, Seq(
        statusOf(etagfs(c)), statusOf(new Path("file://" + local.getAbsolutePath)))))
    val delegate = new FixedFileIndex(listing)
    val index = new EtagFileIndex(delegate, conf)
  }

  test("attaches the MD5 etag of every etagfs file and null for files without one") {
    val f = new Fixture
    assert(etagsOf(f.index.listFiles(Nil, Nil)) == Map(
      "a.txt" -> EtagLocalFileSystem.md5Hex(f.a),
      "b.txt" -> EtagLocalFileSystem.md5Hex(f.b),
      "c.txt" -> EtagLocalFileSystem.md5Hex(f.c),
      "local.txt" -> null))
  }

  test("keeps existing metadata entries on each file") {
    val f = new Fixture
    val tagged = f.listing.map(d => d.copy(files = d.files.map(_.copy(metadata = Map("other" -> 1)))))
    val index = new EtagFileIndex(new FixedFileIndex(tagged), conf)
    index.listFiles(Nil, Nil).flatMap(_.files).foreach { file =>
      assert(file.metadata("other") == 1)
      assert(file.metadata.contains(EtagFileFormats.ETAG_FIELD_NAME))
    }
  }

  test("lists each directory once, and again only after refresh") {
    val f = new Fixture
    val before = EtagLocalFileSystem.listStatusCalls.get()
    f.index.listFiles(Nil, Nil)
    assert(EtagLocalFileSystem.listStatusCalls.get() == before + 2, "two etagfs directories")
    f.index.listFiles(Nil, Nil)
    assert(EtagLocalFileSystem.listStatusCalls.get() == before + 2, "cached")
    f.index.refresh()
    assert(f.delegate.refreshCount == 1)
    f.index.listFiles(Nil, Nil)
    assert(EtagLocalFileSystem.listStatusCalls.get() == before + 4, "relisted after refresh")
  }

  test("applies etag filters itself and keeps them away from the delegate") {
    val f = new Fixture
    val etagFilter = EqualTo(etagAttribute, Literal(EtagLocalFileSystem.md5Hex(f.b)))
    val otherFilter = EqualTo(AttributeReference("value", org.apache.spark.sql.types.LongType)(), Literal(1L))
    val result = f.index.listFiles(Nil, Seq(etagFilter, otherFilter))
    assert(f.delegate.lastDataFilters == Seq(otherFilter))
    assert(result.flatMap(_.files).map(_.getPath.getName) == Seq("b.txt"))
  }

  test("delegates the remaining FileIndex members") {
    val f = new Fixture
    assert(f.index.rootPaths == f.delegate.rootPaths)
    assert(f.index.inputFiles.toSeq == f.delegate.inputFiles.toSeq)
    assert(f.index.sizeInBytes == f.delegate.sizeInBytes)
    assert(f.index.partitionSchema == f.delegate.partitionSchema)
    assert(f.index.metadataOpsTimeNs == f.delegate.metadataOpsTimeNs)
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
sbt "testOnly com.example.spark.etag.EtagFileIndexSuite"
```
Expected: compilation error, `not found: type EtagFileIndex`.

- [ ] **Step 3: Write the implementation**

`src/main/scala/com/example/spark/etag/EtagFileIndex.scala`:
```scala
package com.example.spark.etag

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{EtagSource, Path}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{FileIndex, FileStatusWithMetadata, PartitionDirectory}
import org.apache.spark.sql.types.StructType

/**
 * Wraps any FileIndex and attaches each listed file's ETag under the key "etag" in its metadata
 * map, which is where Spark reads file-constant `_metadata` fields from.
 *
 * Spark's own listing drops the ETag (statuses are re-wrapped and serialised), so this index lists
 * every directory again with Hadoop's `listStatus`, once per directory per index instance. For
 * S3A that is one LIST request per directory, and the ETag comes straight from the listing.
 *
 * @param delegate the index that does the real work: partition discovery, pruning, caching.
 * @param hadoopConf configuration used to obtain the FileSystem for each directory.
 */
class EtagFileIndex(val delegate: FileIndex, hadoopConf: Configuration) extends FileIndex {

  private val etagsByDirectory = new ConcurrentHashMap[Path, Map[Path, String]]()

  override def rootPaths: Seq[Path] = delegate.rootPaths

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val pruner = new EtagFilePruner(dataFilters)
    // The delegate would fail on filters mentioning etag (see EtagFilePruner), so hold them back.
    val delegateFilters = dataFilters.filterNot(pruner.etagFilters.contains)
    delegate.listFiles(partitionFilters, delegateFilters).map { directory =>
      pruner.prune(directory.copy(files = directory.files.map(attachEtag)))
    }
  }

  override def inputFiles: Array[String] = delegate.inputFiles

  override def refresh(): Unit = {
    etagsByDirectory.clear()
    delegate.refresh()
  }

  override def sizeInBytes: Long = delegate.sizeInBytes

  override def partitionSchema: StructType = delegate.partitionSchema

  override def metadataOpsTimeNs: Option[Long] = delegate.metadataOpsTimeNs

  private def attachEtag(file: FileStatusWithMetadata): FileStatusWithMetadata = {
    val path = file.getPath
    val etag = Option(path.getParent).flatMap(etagsIn(_).get(path)).orNull
    file.copy(metadata = file.metadata + (EtagFileFormats.ETAG_FIELD_NAME -> etag))
  }

  private def etagsIn(directory: Path): Map[Path, String] =
    etagsByDirectory.computeIfAbsent(directory, (d: Path) => listEtags(d))

  private def listEtags(directory: Path): Map[Path, String] = {
    val fs = directory.getFileSystem(hadoopConf)
    fs.listStatus(directory).iterator.flatMap { status =>
      status match {
        case withEtag: EtagSource if withEtag.getEtag != null => Some(status.getPath -> withEtag.getEtag)
        case _ => None
      }
    }.toMap
  }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:
```bash
sbt "testOnly com.example.spark.etag.EtagFileIndexSuite"
```
Expected: 5 tests passed.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "Add EtagFileIndex that attaches etags during listing

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Analyzer rule, extension entry point, end-to-end tests

**Files:**
- Create: `src/main/scala/com/example/spark/etag/EtagMetadataRule.scala`
- Create: `src/main/scala/com/example/spark/etag/S3EtagExtension.scala`
- Create: `src/test/scala/com/example/spark/etag/EtagSparkSession.scala`
- Test: `src/test/scala/com/example/spark/etag/EtagMetadataColumnSuite.scala`

**Interfaces:**
- Consumes: `EtagFileIndex(delegate, hadoopConf)` (Task 4), `EtagFileFormats.replacementFor` (Task 1), `EtagLocalFileSystem` (Task 2).
- Produces:
  - `class EtagMetadataRule(session: SparkSession) extends Rule[LogicalPlan]`
  - `object EtagMetadataRule { val EnabledKey = "spark.sql.s3etag.enabled"; val SchemesKey = "spark.sql.s3etag.schemes" }`
  - `class S3EtagExtension extends (SparkSessionExtensions => Unit)` — the class named in `spark.sql.extensions`.

- [ ] **Step 1: Write the shared session trait**

`src/test/scala/com/example/spark/etag/EtagSparkSession.scala`:
```scala
package com.example.spark.etag

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** A local SparkSession with the extension installed and the etagfs scheme enabled. */
trait EtagSparkSession extends BeforeAndAfterAll { this: Suite =>

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("spark-s3-etag-tests")
      .config("spark.sql.extensions", classOf[S3EtagExtension].getName)
      .config("spark.hadoop.fs.etagfs.impl", classOf[EtagLocalFileSystem].getName)
      .config(EtagMetadataRule.SchemesKey, EtagLocalFileSystem.Scheme)
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("etag-warehouse").toString)
      // Keeps the physical plan a plain tree so tests can find the FileSourceScanExec.
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try spark.stop()
    finally super.afterAll()
  }

  /** Runs `body` with a session config set, then restores the previous value. */
  protected def withConf[T](key: String, value: String)(body: => T): T = {
    val previous = spark.conf.getOption(key)
    spark.conf.set(key, value)
    try body
    finally previous match {
      case Some(v) => spark.conf.set(key, v)
      case None => spark.conf.unset(key)
    }
  }
}
```

- [ ] **Step 2: Write the failing end-to-end test**

`src/test/scala/com/example/spark/etag/EtagMetadataColumnSuite.scala`:
```scala
package com.example.spark.etag

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite

class EtagMetadataColumnSuite extends AnyFunSuite with EtagSparkSession {

  private def newDir(): File = Files.createTempDirectory("etag-e2e").toFile

  private def etagfs(dir: File): String = "etagfs://" + dir.getAbsolutePath

  /** Writes three data files of the given format and returns file name -> expected etag. */
  private def writeFiles(format: String, dir: File): Map[String, String] = {
    spark.range(0, 30).withColumn("value", col("id") * 2)
      .repartition(3).write.format(format).save(dir.getAbsolutePath)
    expectedEtags(dir)
  }

  /** MD5 of every data file under dir, recursively, keyed by file name. */
  private def expectedEtags(dir: File): Map[String, String] = {
    val (dirs, files) = dir.listFiles.toSeq.partition(_.isDirectory)
    files.filter(_.getName.startsWith("part-")).map(f => f.getName -> EtagLocalFileSystem.md5Hex(f)).toMap ++
      dirs.flatMap(expectedEtags)
  }

  private def actualEtags(df: DataFrame): Map[String, String] =
    df.select(col("_metadata.file_name"), col("_metadata.etag")).distinct()
      .collect().map(r => r.getString(0) -> r.getString(1)).toMap

  private def metadataFieldNames(df: DataFrame): Seq[String] =
    df.select("_metadata").schema.head.dataType.asInstanceOf[StructType].fieldNames.toSeq

  Seq("parquet", "orc", "csv", "json").foreach { format =>
    test(s"$format: DataFrame read exposes _metadata.etag equal to the file MD5") {
      val dir = newDir()
      val expected = writeFiles(format, dir)
      assert(expected.size == 3)
      val df = spark.read.format(format).load(etagfs(dir))
      assert(actualEtags(df) == expected)
    }
  }

  test("SQL path form resolves _metadata.etag") {
    val dir = newDir()
    val expected = writeFiles("parquet", dir)
    val df = spark.sql(
      s"SELECT _metadata.file_name AS file_name, _metadata.etag AS etag FROM parquet.`${etagfs(dir)}`")
    val actual = df.distinct().collect().map(r => r.getString(0) -> r.getString(1)).toMap
    assert(actual == expected)
  }

  test("catalog table with an etagfs location resolves _metadata.etag") {
    val dir = newDir()
    val expected = writeFiles("parquet", dir)
    spark.sql(s"CREATE TABLE etag_table USING parquet LOCATION '${etagfs(dir)}'")
    try assert(actualEtags(spark.table("etag_table")) == expected)
    finally spark.sql("DROP TABLE etag_table")
  }

  test("a filter on _metadata.etag prunes files before the scan") {
    val dir = newDir()
    val expected = writeFiles("parquet", dir)
    val (name, etag) = expected.head
    val filtered = spark.read.parquet(etagfs(dir)).filter(col("_metadata.etag") === etag)
    assert(actualEtags(filtered) == Map(name -> etag))
    val scan = filtered.queryExecution.executedPlan.collectFirst { case s: FileSourceScanExec => s }.get
    filtered.collect()
    assert(scan.driverMetrics("numFiles").value == 1)
  }

  test("partitioned directory keeps partition columns and per-file etags") {
    val dir = newDir()
    spark.range(0, 30).withColumn("p", col("id") % 2).repartition(2)
      .write.partitionBy("p").parquet(dir.getAbsolutePath)
    val expected = expectedEtags(dir)
    assert(expected.nonEmpty)
    val df = spark.read.parquet(etagfs(dir))
    assert(actualEtags(df) == expected)
    val onePartition = df.filter(col("p") === 1)
    assert(onePartition.select("p").distinct().collect().map(_.getLong(0)).toSeq == Seq(1L))
    val partitionFiles = expectedEtags(new File(dir, "p=1"))
    assert(actualEtags(onePartition) == partitionFiles)
  }

  test("a path on a scheme that is not enabled keeps the default _metadata shape") {
    val dir = newDir()
    writeFiles("parquet", dir)
    val df = spark.read.parquet(dir.getAbsolutePath)
    assert(!metadataFieldNames(df).contains("etag"))
  }

  test("an enabled scheme without EtagSource support yields a null etag") {
    val dir = newDir()
    writeFiles("parquet", dir)
    withConf(EtagMetadataRule.SchemesKey, "file") {
      val df = spark.read.parquet("file://" + dir.getAbsolutePath)
      assert(metadataFieldNames(df).contains("etag"))
      assert(actualEtags(df).values.forall(_ == null))
    }
  }

  test("spark.sql.s3etag.enabled=false disables the rewrite") {
    val dir = newDir()
    writeFiles("parquet", dir)
    withConf(EtagMetadataRule.EnabledKey, "false") {
      val df = spark.read.parquet(etagfs(dir))
      assert(!metadataFieldNames(df).contains("etag"))
    }
  }

  test("the rewritten relation keeps data columns and ordinary filters working") {
    val dir = newDir()
    writeFiles("parquet", dir)
    val df = spark.read.parquet(etagfs(dir)).filter(col("value") > 40)
    assert(df.count() == 9)
    assert(df.select("id", "value", "_metadata.etag").columns.toSeq == Seq("id", "value", "etag"))
  }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run:
```bash
sbt "testOnly com.example.spark.etag.EtagMetadataColumnSuite"
```
Expected: compilation error, `not found: type S3EtagExtension`.

- [ ] **Step 4: Write the rule**

`src/main/scala/com/example/spark/etag/EtagMetadataRule.scala`:
```scala
package com.example.spark.etag

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.FileSourceMetadataAttribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
 * Rewrites file relations so their `_metadata` struct gains an `etag` field.
 *
 * Injected as a resolution rule, it runs in the same analyzer iteration that turns a path or
 * table into a LogicalRelation and before `_metadata` references are resolved against it. A
 * relation is rewritten when its format is one of Spark's built-in Parquet/ORC/CSV/JSON formats
 * and every root path uses a scheme listed in `spark.sql.s3etag.schemes`.
 */
class EtagMetadataRule(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (!enabled) plan
    else plan.resolveOperatorsUp {
      case relation: LogicalRelation =>
        relation.relation match {
          case fsRelation: HadoopFsRelation if shouldRewrite(fsRelation) => rewrite(relation, fsRelation)
          case _ => relation
        }
    }

  private def enabled: Boolean =
    session.conf.get(EtagMetadataRule.EnabledKey, "true").trim.toBoolean

  private def enabledSchemes: Set[String] =
    session.conf.get(EtagMetadataRule.SchemesKey, EtagMetadataRule.DefaultSchemes)
      .split(",").map(_.trim.toLowerCase).filter(_.nonEmpty).toSet

  private def shouldRewrite(fsRelation: HadoopFsRelation): Boolean = {
    val schemes = enabledSchemes
    val roots = fsRelation.location.rootPaths
    !fsRelation.location.isInstanceOf[EtagFileIndex] &&
      EtagFileFormats.replacementFor(fsRelation.fileFormat).isDefined &&
      roots.nonEmpty &&
      roots.forall(root => schemes.contains(schemeOf(root)))
  }

  private def schemeOf(path: Path): String =
    Option(path.toUri.getScheme).getOrElse("").toLowerCase

  private def rewrite(relation: LogicalRelation, fsRelation: HadoopFsRelation): LogicalRelation = {
    val rewrittenFsRelation = fsRelation.copy(
      location = new EtagFileIndex(fsRelation.location, session.sessionState.newHadoopConf()),
      fileFormat = EtagFileFormats.replacementFor(fsRelation.fileFormat).get)(fsRelation.sparkSession)

    // If Spark already added a _metadata attribute to this relation's output, its struct type
    // lacks etag. Drop it and let the new relation add its own, whose type includes etag.
    val (metadataAttributes, dataAttributes) =
      relation.output.partition(attr => FileSourceMetadataAttribute.unapply(attr).isDefined)
    val rewritten = relation.copy(relation = rewrittenFsRelation, output = dataAttributes)
    rewritten.copyTagsFrom(relation)
    if (metadataAttributes.isEmpty) rewritten else rewritten.withMetadataColumns()
  }
}

object EtagMetadataRule {
  val EnabledKey: String = "spark.sql.s3etag.enabled"
  val SchemesKey: String = "spark.sql.s3etag.schemes"
  val DefaultSchemes: String = "s3a"
}
```

- [ ] **Step 5: Write the extension entry point**

`src/main/scala/com/example/spark/etag/S3EtagExtension.scala`:
```scala
package com.example.spark.etag

import org.apache.spark.sql.SparkSessionExtensions

/**
 * Entry point for `spark.sql.extensions`:
 *
 * {{{
 * --conf spark.sql.extensions=com.example.spark.etag.S3EtagExtension
 * }}}
 */
class S3EtagExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit =
    extensions.injectResolutionRule(session => new EtagMetadataRule(session))
}
```

- [ ] **Step 6: Run the end-to-end tests**

Run:
```bash
sbt "testOnly com.example.spark.etag.EtagMetadataColumnSuite"
```
Expected: 12 tests passed. If `driverMetrics("numFiles")` is not accessible from outside Spark's packages, replace that assertion with `assert(scan.selectedPartitions.totalNumberOfFiles == 1)`; if neither compiles, replace it with `assert(scan.metrics("numFiles").value == 1)`. Keep exactly one of these.

- [ ] **Step 7: Run the whole suite**

Run:
```bash
sbt test
```
Expected: `All tests passed.` with 32 tests across 5 suites.

- [ ] **Step 8: Commit**

```bash
git add src
git commit -m "Add analyzer rule and extension exposing _metadata.etag

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: README, packaged jar, spec sync

**Files:**
- Create: `README.md`
- Modify: `docs/superpowers/specs/2026-09-12-s3-etag-metadata-column-design.md` (pruning paragraph and test-filesystem paragraph)

**Interfaces:**
- Consumes: everything above.
- Produces: `target/scala-2.13/spark-s3-etag_2.13-0.1.0.jar`.

- [ ] **Step 1: Write the README**

`README.md`:
````markdown
# spark-s3-etag

A Spark 4.1 extension that exposes the S3 ETag of every input file as
`_metadata.etag` on Parquet, ORC, CSV and JSON reads.

```sql
SELECT _metadata.file_path, _metadata.etag, *
FROM parquet.`s3a://my-bucket/events/`
```

```python
(spark.read.parquet("s3a://my-bucket/events/")
      .select("_metadata.file_name", "_metadata.etag", "id"))
```

`WHERE _metadata.etag = '...'` is applied while listing, so non-matching
files are never opened.

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
| `spark.sql.s3etag.schemes` | `s3a` | Comma-separated URI schemes whose reads get the `etag` field. |

Both can be changed at runtime with `SET` and take effect for later queries.

## How it works

1. An analyzer rule rewrites each file relation on an enabled scheme: the file
   index is wrapped, and the file format is swapped for a subclass that adds
   `etag` to the `_metadata` struct.
2. While Spark lists files for a query, the wrapper issues one `listStatus`
   per directory and reads the ETag from Hadoop's `EtagSource` statuses. That
   is one S3 LIST request per directory, cached for the life of the plan.
3. Filters on `_metadata.etag` are applied by the wrapper during listing.

## Checking against a real bucket

```bash
aws s3 cp hello.csv s3://my-bucket/check/hello.csv
aws s3api head-object --bucket my-bucket --key check/hello.csv --query ETag
```

```python
spark.read.csv("s3a://my-bucket/check/").select("_metadata.etag").show(truncate=False)
```

The two values match, minus the surrounding quotes S3 prints.

## Limitations

* Objects uploaded in several parts have an ETag that is not the MD5 of the
  object. The column reports whatever S3 returns.
* Only Data Source V1 file relations are rewritten. The defaults in Spark 4.1
  route Parquet, ORC, CSV and JSON through V1.
* Streaming reads are not rewritten.
* Filesystems whose file statuses do not implement `EtagSource` yield a null
  `etag` (for example plain `file://`, or vendor S3 clients).
* Wrapping the file index hides its concrete class from Spark's
  `PruneFileSourcePartitions` optimizer rule. Partition pruning still happens
  during listing, but optimizer statistics for partitioned catalog tables can
  be less precise.
````

- [ ] **Step 2: Sync the spec with what was learned during planning**

In `docs/superpowers/specs/2026-09-12-s3-etag-metadata-column-design.md`:

Replace the bullet
```
* `FilePruningRunner` evaluates filters on constant metadata fields at
  planning time, so files can be pruned by ETag.
```
with
```
* `FilePruningRunner` evaluates filters on constant metadata fields at
  planning time, but it builds the metadata row with
  `FileFormat.createMetadataInternalRow`, which asserts that every field is
  one of the built-in four. A filter on `etag` therefore must not reach the
  wrapped index; `EtagFileIndex` strips such filters and applies them itself
  through `EtagFilePruner` after attaching the etags.
```

Replace the sentence in the `EtagLocalFileSystem` paragraph
```
Its `listStatus`,
`getFileStatus` and `listLocatedStatus` wrap each file status in a subclass
```
with
```
Its `listStatus` and
`getFileStatus` wrap each file status in a subclass
```

Add to the `## Components` section, after `EtagFileIndex`:
```
### `EtagFilePruner(dataFilters: Seq[Expression])`

Exposes `etagFilters`, the input filters that reference the `etag` metadata
attribute, and `prune(directory)`, which drops files failing those filters
whose references are all file-constant metadata attributes other than
`file_block_start` and `file_block_length`. Binding and evaluation mirror
Spark's `FilePruningRunner`, but the metadata row is built with
`FileFormat.updateMetadataInternalRow` over a `PartitionedFile`, which
supports custom constant fields.
```

- [ ] **Step 3: Build the jar and check its contents**

Run:
```bash
sbt clean package
```
Expected: `target/scala-2.13/spark-s3-etag_2.13-0.1.0.jar` is written.

Run:
```bash
unzip -l target/scala-2.13/spark-s3-etag_2.13-0.1.0.jar | grep -c "com/example/spark/etag/"
```
Expected: a count of at least 8 class entries and no entries from other packages (e.g. no `org/apache/`).

- [ ] **Step 4: Run the full suite one last time**

Run:
```bash
sbt test
```
Expected: `All tests passed.`

- [ ] **Step 5: Commit**

```bash
git add README.md docs
git commit -m "Add README and sync spec with pruning design

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```
