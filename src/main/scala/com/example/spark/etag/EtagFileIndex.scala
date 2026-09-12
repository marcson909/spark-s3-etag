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
    // The delegate would fail on filters mentioning etag or user_metadata (see EtagFilePruner), so hold them back.
    val delegateFilters = dataFilters.filterNot(pruner.heldBackFilters.contains)
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
