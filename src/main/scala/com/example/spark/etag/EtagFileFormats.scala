package com.example.spark.etag

import org.apache.spark.sql.catalyst.expressions.FileSourceConstantMetadataStructField
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{StringType, StructField}

/**
 * The `_metadata.etag` and `_metadata.user_metadata` fields and the file formats that expose them.
 *
 * Spark builds the `_metadata` struct from `FileFormat.metadataSchemaFields`. A "constant" field
 * is filled per file by looking its name up in the metadata map that the FileIndex attached to the
 * file (see EtagFileIndex), so the formats below only need to declare the fields.
 *
 * `user_metadata` is a JSON object string rather than a map because Spark only allows primitive,
 * decimal, binary, string and calendar-interval metadata fields
 * (`FileSourceMetadataAttribute.isSupportedType`). Read one key in SQL with
 * `from_json(_metadata.user_metadata, 'map<string,string>')['mtime']` or
 * `get_json_object(_metadata.user_metadata, '$.mtime')`.
 */
object EtagFileFormats {
  val ETAG_FIELD_NAME: String = "etag"
  val USER_METADATA_FIELD_NAME: String = "user_metadata"

  /** Nullable because not every filesystem, and not every S3 object listing, provides an ETag. */
  val ETAG_FIELD: StructField =
    FileSourceConstantMetadataStructField(ETAG_FIELD_NAME, StringType, nullable = true)

  /**
   * S3 user-defined object metadata as a JSON object whose keys are the metadata keys without
   * the `x-amz-meta-` prefix (rclone's mtime is under "mtime"); null when there is none.
   */
  val USER_METADATA_FIELD: StructField =
    FileSourceConstantMetadataStructField(USER_METADATA_FIELD_NAME, StringType, nullable = true)

  /** Fields this extension adds. Filters on them must not reach Spark's own file pruner. */
  val CUSTOM_FIELD_NAMES: Set[String] = Set(ETAG_FIELD_NAME, USER_METADATA_FIELD_NAME)

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

  private[etag] def withCustomFields(fields: Seq[StructField]): Seq[StructField] =
    fields :+ ETAG_FIELD :+ USER_METADATA_FIELD
}

// Each subclass overrides equals/hashCode to compare by exact class. The built-in Parquet and
// ORC formats treat any subclass as equal to themselves, which would let Spark's plan cache
// mistake a rewritten relation for the original.

class EtagParquetFileFormat extends ParquetFileFormat {
  override def metadataSchemaFields: Seq[StructField] =
    EtagFileFormats.withCustomFields(super.metadataSchemaFields)
  override def equals(other: Any): Boolean = other != null && other.getClass == getClass
  override def hashCode(): Int = getClass.hashCode()
}

class EtagOrcFileFormat extends OrcFileFormat {
  override def metadataSchemaFields: Seq[StructField] =
    EtagFileFormats.withCustomFields(super.metadataSchemaFields)
  override def equals(other: Any): Boolean = other != null && other.getClass == getClass
  override def hashCode(): Int = getClass.hashCode()
}

class EtagCsvFileFormat extends CSVFileFormat {
  override def metadataSchemaFields: Seq[StructField] =
    EtagFileFormats.withCustomFields(super.metadataSchemaFields)
  override def equals(other: Any): Boolean = other != null && other.getClass == getClass
  override def hashCode(): Int = getClass.hashCode()
}

class EtagJsonFileFormat extends JsonFileFormat {
  override def metadataSchemaFields: Seq[StructField] =
    EtagFileFormats.withCustomFields(super.metadataSchemaFields)
  override def equals(other: Any): Boolean = other != null && other.getClass == getClass
  override def hashCode(): Int = getClass.hashCode()
}
