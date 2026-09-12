package com.example.spark.etag

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BoundReference, Expression, FileSourceConstantMetadataAttribute, GenericInternalRow, Predicate}
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionDirectory}

/**
 * Prunes listed files with data filters that mention the custom metadata columns `etag` and
 * `user_metadata`.
 *
 * Spark's own FilePruningRunner handles filters on file-constant metadata columns, but it builds
 * the metadata row with FileFormat.createMetadataInternalRow, which only knows the built-in fields
 * and fails an assertion on any other. So EtagFileIndex keeps `heldBackFilters` away from the
 * wrapped index and calls `prune` here after the custom values have been attached.
 *
 * As in Spark, only filters whose references are all file-constant metadata columns can prune,
 * and `file_block_start` / `file_block_length` are excluded because splits are not known yet.
 * A filter such as `get_json_object(user_metadata, '$.mtime') > '...'` qualifies: its only
 * reference is the constant `user_metadata` column.
 */
class EtagFilePruner(dataFilters: Seq[Expression]) {

  /** Every filter that references a custom field. None of these may reach the wrapped FileIndex. */
  val heldBackFilters: Seq[Expression] = dataFilters.filter(_.references.exists(isCustomField))

  private val prunableFilter: Option[Expression] =
    heldBackFilters.filter(_.references.forall(isPrunableMetadata)).reduceOption(And)

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

  private def isCustomField(attr: Attribute): Boolean = attr match {
    case FileSourceConstantMetadataAttribute(metadata) =>
      EtagFileFormats.CUSTOM_FIELD_NAMES.contains(metadata.name)
    case _ => false
  }

  private def isPrunableMetadata(attr: Attribute): Boolean = attr match {
    case FileSourceConstantMetadataAttribute(metadata) =>
      metadata.name != FileFormat.FILE_BLOCK_START && metadata.name != FileFormat.FILE_BLOCK_LENGTH
    case _ => false
  }
}
