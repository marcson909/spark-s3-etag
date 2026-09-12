package com.example.spark.etag

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** A local SparkSession with the extension installed and the etagfs scheme enabled. */
trait EtagSparkSession extends BeforeAndAfterAll { this: Suite =>

  protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("spark-s3-etag-tests")
      .config("spark.sql.extensions", classOf[S3EtagExtension].getName)
      .config("spark.hadoop.fs.etagfs.impl", classOf[EtagLocalFileSystem].getName)
      .config(EtagMetadataRule.SchemesKey, EtagLocalFileSystem.Scheme)
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("etag-warehouse").toString)
      // Keeps the physical plan a plain tree so tests can find the FileSourceScanExec.
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try spark.stop()
    finally super.afterAll()
  }

  /** Runs `body` with a session config set, then restores the previous value. */
  protected def withConf[T](key: String, value: String)(body: => T): T = {
    val previous = spark.conf.getOption(key)
    spark.conf.set(key, value)
    try body
    finally previous match {
      case Some(v) => spark.conf.set(key, v)
      case None => spark.conf.unset(key)
    }
  }
}
