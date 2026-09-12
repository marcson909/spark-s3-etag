package com.example.spark.etag

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.{EtagSource, FileStatus, Path, RawLocalFileSystem}

/**
 * A file status that also carries an ETag, the way Hadoop's S3AFileStatus does.
 *
 * Deliberately does not use the `FileStatus(FileStatus)` copy constructor: that constructor
 * eagerly reads `other.getPermission()`/`getOwner()`/`getGroup()`, which on a
 * `RawLocalFileSystem` status are lazily loaded from the filesystem by rebuilding a
 * `java.io.File` out of the status's path URI. Since `EtagLocalFileSystem` reports its own
 * `etagfs` URI, that rebuild fails with "URI scheme is not file". Copying only the fields the
 * tests need (length, directory-ness, replication, block size, modification time, path) avoids
 * triggering that lazy load.
 */
class EtagFileStatus(other: FileStatus, etag: String)
    extends FileStatus(
      other.getLen,
      other.isDirectory,
      other.getReplication,
      other.getBlockSize,
      other.getModificationTime,
      other.getPath
    ) with EtagSource {
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
