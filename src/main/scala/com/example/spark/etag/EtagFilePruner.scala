package com.example.spark.etag

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BoundReference, Expression, FileSourceConstantMetadataAttribute, GenericInternalRow, Predicate}
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionDirectory}

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
