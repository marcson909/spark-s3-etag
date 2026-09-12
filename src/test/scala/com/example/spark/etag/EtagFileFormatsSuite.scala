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
      val last = fields.dropRight(1).last
      assert(last.name == "etag", s"${format.getClass.getSimpleName} second-to-last field")
      assert(last.dataType == StringType)
      assert(last.nullable)
      assert(FileSourceConstantMetadataStructField.unapply(last).isDefined,
        "etag must be a file-constant metadata field")
      assert(fields.map(_.name).contains(FileFormat.FILE_PATH), "built-in fields must be kept")
    }
  }

  test("parquet keeps its generated row_index field ahead of etag") {
    val names = (new EtagParquetFileFormat).metadataSchemaFields.map(_.name)
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
    assert((new EtagParquetFileFormat).hashCode == (new EtagParquetFileFormat).hashCode)
  }

  test("every etag format declares user_metadata as a nullable string right after etag") {
    etagFormats.foreach { format =>
      val fields = format.metadataSchemaFields
      assert(fields.takeRight(2).map(_.name) == Seq("etag", "user_metadata"))
      val userMetadata = fields.last
      assert(userMetadata.dataType == StringType)
      assert(userMetadata.nullable)
      assert(FileSourceConstantMetadataStructField.unapply(userMetadata).isDefined)
    }
  }

  test("CUSTOM_FIELD_NAMES lists exactly the fields this extension adds") {
    assert(EtagFileFormats.CUSTOM_FIELD_NAMES == Set("etag", "user_metadata"))
  }
}
