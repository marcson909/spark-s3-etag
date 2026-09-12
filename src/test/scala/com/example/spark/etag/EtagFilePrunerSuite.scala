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
