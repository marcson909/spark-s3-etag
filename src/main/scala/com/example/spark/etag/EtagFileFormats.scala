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
