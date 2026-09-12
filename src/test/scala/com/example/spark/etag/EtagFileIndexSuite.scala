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
