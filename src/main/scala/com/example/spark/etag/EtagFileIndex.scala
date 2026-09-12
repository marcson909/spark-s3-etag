package com.example.spark.etag

import java.nio.charset.StandardCharsets
import java.util.{Locale, Map => JMap}
import java.util.concurrent.{Callable, ConcurrentHashMap, ExecutionException, Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{EtagSource, Path}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.{FileIndex, FileStatusWithMetadata, PartitionDirectory}
import org.apache.spark.sql.types.StructType

/**
 * Wraps any FileIndex and attaches two values to each listed file's metadata map, which is where
 * Spark reads file-constant `_metadata` fields from:
 *
 *  - "etag": the object's ETag, from one Hadoop `listStatus` per directory (one S3 LIST each).
 *    Spark's own listing drops the ETag (statuses are re-wrapped and serialised), so this index
 *    lists every directory again, once per directory per index instance.
 *  - "user_metadata": S3 user-defined metadata as a JSON object string, from one `getXAttrs` per
 *    file (one S3 HEAD each), fetched with a bounded thread pool. Skipped, and attached as null,
 *    when `fetchUserMetadata` is false.
 *
 * Both caches are cleared by `refresh()`. Listing failures fail the query, the same way they
 * would for Spark's own listing, and so do HEAD failures unless `ignoreMissingFiles` is set.
 *
 * @param delegate the index that does the real work: partition discovery, pruning, caching.
 * @param hadoopConf configuration used to obtain the FileSystem for each path.
 * @param fetchUserMetadata whether to issue the per-file requests for user metadata.
 * @param ignoreMissingFiles whether an object that disappeared after the listing yields null
 *                           user metadata instead of failing the query.
 */
class EtagFileIndex(
    val delegate: FileIndex,
    val hadoopConf: Configuration,
    val fetchUserMetadata: Boolean,
    val ignoreMissingFiles: Boolean = false) extends FileIndex {

  private val etagsByDirectory = new ConcurrentHashMap[Path, Map[Path, String]]()

  /** JSON per file; None for an object without user metadata or a filesystem without xattrs. */
  private val userMetadataByPath = new ConcurrentHashMap[Path, Option[String]]()

  override def rootPaths: Seq[Path] = delegate.rootPaths

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val pruner = new EtagFilePruner(dataFilters)
    // The delegate would fail on filters mentioning etag or user_metadata (see EtagFilePruner),
    // so hold them back.
    val delegateFilters = dataFilters.filterNot(pruner.heldBackFilters.contains)
    val listed = delegate.listFiles(partitionFilters, delegateFilters)
    if (fetchUserMetadata) fetchMissingUserMetadata(listed.flatMap(_.files.map(_.getPath)))
    listed.map { directory =>
      pruner.prune(directory.copy(files = directory.files.map(attachValues)))
    }
  }

  override def inputFiles: Array[String] = delegate.inputFiles

  override def refresh(): Unit = {
    etagsByDirectory.clear()
    userMetadataByPath.clear()
    delegate.refresh()
  }

  override def sizeInBytes: Long = delegate.sizeInBytes

  override def partitionSchema: StructType = delegate.partitionSchema

  override def metadataOpsTimeNs: Option[Long] = delegate.metadataOpsTimeNs

  override def equals(other: Any): Boolean = other match {
    case that: EtagFileIndex =>
      delegate == that.delegate && fetchUserMetadata == that.fetchUserMetadata &&
        ignoreMissingFiles == that.ignoreMissingFiles
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(delegate, Boolean.box(fetchUserMetadata), Boolean.box(ignoreMissingFiles))

  private def attachValues(file: FileStatusWithMetadata): FileStatusWithMetadata = {
    val path = file.getPath
    val etag = Option(path.getParent).flatMap(etagsIn(_).get(path)).orNull
    val userMetadata =
      if (fetchUserMetadata) Option(userMetadataByPath.get(path)).flatten.orNull else null
    file.copy(metadata = file.metadata +
      (EtagFileFormats.ETAG_FIELD_NAME -> etag) +
      (EtagFileFormats.USER_METADATA_FIELD_NAME -> userMetadata))
  }

  private def etagsIn(directory: Path): Map[Path, String] =
    etagsByDirectory.computeIfAbsent(directory, (d: Path) => listEtags(d))

  private def listEtags(directory: Path): Map[Path, String] = {
    val fs = directory.getFileSystem(hadoopConf)
    // A listing failure fails the query, as it would for Spark's own listing.
    fs.listStatus(directory).iterator.flatMap { status =>
      status match {
        case withEtag: EtagSource if withEtag.getEtag != null => Some(status.getPath -> withEtag.getEtag)
        case _ => None
      }
    }.toMap
  }

  /** Reads user metadata for every path not cached yet, `UserMetadataThreads` requests at a time. */
  private def fetchMissingUserMetadata(paths: Seq[Path]): Unit = {
    val missing = paths.distinct.filterNot(userMetadataByPath.containsKey)
    if (missing.nonEmpty) {
      val pool = Executors.newFixedThreadPool(EtagFileIndex.UserMetadataThreads, EtagFileIndex.daemonThreads)
      try {
        val futures = missing.map { path =>
          pool.submit(new Callable[Option[String]] {
            override def call(): Option[String] = readUserMetadata(path)
          })
        }
        missing.zip(futures).foreach { case (path, future) =>
          val json =
            try future.get()
            catch {
              case e: ExecutionException =>
                val cause = Option(e.getCause).getOrElse(e)
                throw new java.io.IOException(s"Failed to read user metadata for $path", cause)
            }
          userMetadataByPath.put(path, json)
        }
      } finally {
        pool.shutdownNow()
      }
    }
  }

  /** The object's user metadata as JSON; None when it has none or the filesystem has no xattrs. */
  private def readUserMetadata(path: Path): Option[String] = {
    val fs = path.getFileSystem(hadoopConf)
    val attributes: JMap[String, Array[Byte]] =
      try fs.getXAttrs(path)
      catch {
        case _: UnsupportedOperationException => null
        // The object went away between the listing and this HEAD.
        case _: java.io.FileNotFoundException if ignoreMissingFiles => null
      }
    if (attributes == null) None
    else {
      val values = attributes.asScala.iterator.flatMap { case (name, bytes) =>
        EtagFileIndex.userMetadataKey(name).map(_ -> new String(bytes, StandardCharsets.UTF_8))
      }.toMap
      if (values.isEmpty) None else Some(UserMetadataJson.encode(values))
    }
  }
}

object EtagFileIndex {
  /** Concurrent per-file requests while fetching user metadata on the driver. */
  val UserMetadataThreads: Int = 32

  /** S3A prefixes every object header it exposes as an xattr with this. */
  private val HeaderPrefix = "header."

  /** The prefix S3 puts on user metadata on the wire; S3A has usually stripped it already. */
  private val UserMetadataHeaderPrefix = "x-amz-meta-"

  /**
   * Standard headers S3A exposes under the same prefix, from HeaderProcessing.XA_STANDARD_HEADERS
   * in Hadoop 3.4 (lower-cased). Anything else under "header." is user metadata.
   */
  private val StandardHeaders: Set[String] = Set(
    "cache-control", "content-disposition", "content-encoding", "content-language",
    "content-length", "content-md5", "content-range", "content-type", "etag", "last-modified",
    "x-amz-archive-status", "x-amz-object-lock-legal-hold", "x-amz-object-lock-mode",
    "x-amz-object-lock-retain-until-date", "x-amz-replication-status", "x-amz-version-id",
    "x-amz-server-side-encryption", "x-amz-server-side-encryption-aws-kms-key-id",
    "x-amz-storage-class").map(HeaderPrefix + _)

  /** The user metadata key an xattr name stands for, or None for anything that is not user metadata. */
  def userMetadataKey(xattrName: String): Option[String] = {
    val lower = xattrName.toLowerCase(Locale.ROOT)
    if (!lower.startsWith(HeaderPrefix) || StandardHeaders.contains(lower)) None
    else Some(lower.stripPrefix(HeaderPrefix).stripPrefix(UserMetadataHeaderPrefix))
  }

  private val daemonThreads: ThreadFactory = new ThreadFactory {
    private val counter = new AtomicInteger(0)
    override def newThread(r: Runnable): Thread = {
      val thread = new Thread(r, s"etag-user-metadata-${counter.incrementAndGet()}")
      thread.setDaemon(true)
      thread
    }
  }
}
