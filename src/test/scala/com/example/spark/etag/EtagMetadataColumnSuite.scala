package com.example.spark.etag

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite

class EtagMetadataColumnSuite extends AnyFunSuite with EtagSparkSession {

  // Spark's writer refuses an existing output path, so name a subdirectory the write will create.
  private def newDir(): File = new File(Files.createTempDirectory("etag-e2e").toFile, "data")

  private def etagfs(dir: File): String = "etagfs://" + dir.getAbsolutePath

  /** Writes three data files of the given format and returns file name -> expected etag. */
  private def writeFiles(format: String, dir: File): Map[String, String] = {
    spark.range(0, 30).withColumn("value", col("id") * 2)
      .repartition(3).write.format(format).save(dir.getAbsolutePath)
    expectedEtags(dir)
  }

  /** MD5 of every data file under dir, recursively, keyed by file name. */
  private def expectedEtags(dir: File): Map[String, String] = {
    val (dirs, files) = dir.listFiles.toSeq.partition(_.isDirectory)
    files.filter(_.getName.startsWith("part-")).map(f => f.getName -> EtagLocalFileSystem.md5Hex(f)).toMap ++
      dirs.flatMap(expectedEtags)
  }

  private def actualEtags(df: DataFrame): Map[String, String] =
    df.select(col("_metadata.file_name"), col("_metadata.etag")).distinct()
      .collect().map(r => r.getString(0) -> r.getString(1)).toMap

  /** MD5 of every data file under dir, recursively, keyed by "<parent dir name>/<file name>". */
  private def expectedEtagsByParent(dir: File): Map[String, String] = {
    val (dirs, files) = dir.listFiles.toSeq.partition(_.isDirectory)
    files.filter(_.getName.startsWith("part-"))
      .map(f => s"${f.getParentFile.getName}/${f.getName}" -> EtagLocalFileSystem.md5Hex(f)).toMap ++
      dirs.flatMap(expectedEtagsByParent)
  }

  /** Etags keyed by "<parent dir name>/<file name>" taken from _metadata.file_path. */
  private def actualEtagsByParent(df: DataFrame): Map[String, String] =
    df.select(col("_metadata.file_path"), col("_metadata.etag")).distinct()
      .collect().map { r =>
        val path = new org.apache.hadoop.fs.Path(r.getString(0))
        s"${path.getParent.getName}/${path.getName}" -> r.getString(1)
      }.toMap

  private def metadataFieldNames(df: DataFrame): Seq[String] =
    df.select("_metadata").schema.head.dataType.asInstanceOf[StructType].fieldNames.toSeq

  Seq("parquet", "orc", "csv", "json").foreach { format =>
    test(s"$format: DataFrame read exposes _metadata.etag equal to the file MD5") {
      val dir = newDir()
      val expected = writeFiles(format, dir)
      assert(expected.size == 3)
      val df = spark.read.format(format).load(etagfs(dir))
      assert(actualEtags(df) == expected)
    }
  }

  test("SQL path form resolves _metadata.etag") {
    val dir = newDir()
    val expected = writeFiles("parquet", dir)
    val df = spark.sql(
      s"SELECT _metadata.file_name AS file_name, _metadata.etag AS etag FROM parquet.`${etagfs(dir)}`")
    val actual = df.distinct().collect().map(r => r.getString(0) -> r.getString(1)).toMap
    assert(actual == expected)
  }

  test("catalog table with an etagfs location resolves _metadata.etag") {
    val dir = newDir()
    val expected = writeFiles("parquet", dir)
    spark.sql(s"CREATE TABLE etag_table USING parquet LOCATION '${etagfs(dir)}'")
    try assert(actualEtags(spark.table("etag_table")) == expected)
    finally spark.sql("DROP TABLE etag_table")
  }

  test("a filter on _metadata.etag prunes files before the scan") {
    val dir = newDir()
    val expected = writeFiles("parquet", dir)
    val (name, etag) = expected.head
    val filtered = spark.read.parquet(etagfs(dir)).filter(col("_metadata.etag") === etag)
    assert(actualEtags(filtered) == Map(name -> etag))
    val scan = filtered.queryExecution.executedPlan.collectFirst { case s: FileSourceScanExec => s }.get
    filtered.collect()
    assert(scan.driverMetrics("numFiles").value == 1)
  }

  test("partitioned directory keeps partition columns and per-file etags") {
    val dir = newDir()
    spark.range(0, 30).withColumn("p", col("id") % 2).repartition(2)
      .write.partitionBy("p").parquet(dir.getAbsolutePath)
    val expected = expectedEtagsByParent(dir)
    assert(expected.nonEmpty)
    val df = spark.read.parquet(etagfs(dir))
    assert(actualEtagsByParent(df) == expected)
    val onePartition = df.filter(col("p") === 1)
    // Partition discovery infers the directory value p=1 as an int, not the written long.
    assert(onePartition.select("p").distinct().collect().map(_.getInt(0)).toSeq == Seq(1))
    val partitionFiles = expectedEtagsByParent(new File(dir, "p=1"))
    assert(actualEtagsByParent(onePartition) == partitionFiles)
  }

  test("a path on a scheme that is not enabled keeps the default _metadata shape") {
    val dir = newDir()
    writeFiles("parquet", dir)
    val df = spark.read.parquet(dir.getAbsolutePath)
    assert(!metadataFieldNames(df).contains("etag"))
  }

  test("an enabled scheme without EtagSource support yields a null etag") {
    val dir = newDir()
    writeFiles("parquet", dir)
    withConf(EtagMetadataRule.SchemesKey, "file") {
      val df = spark.read.parquet("file://" + dir.getAbsolutePath)
      assert(metadataFieldNames(df).contains("etag"))
      assert(actualEtags(df).values.forall(_ == null))
    }
  }

  test("spark.sql.s3etag.enabled=false disables the rewrite") {
    val dir = newDir()
    writeFiles("parquet", dir)
    withConf(EtagMetadataRule.EnabledKey, "false") {
      val df = spark.read.parquet(etagfs(dir))
      assert(!metadataFieldNames(df).contains("etag"))
    }
  }

  test("a non-boolean value of spark.sql.s3etag.enabled is ignored and the rewrite stays on") {
    val dir = newDir()
    writeFiles("parquet", dir)
    withConf(EtagMetadataRule.EnabledKey, "yes") {
      val df = spark.read.parquet(etagfs(dir))
      assert(metadataFieldNames(df).contains("etag"))
    }
  }

  test("the rewritten relation keeps data columns and ordinary filters working") {
    val dir = newDir()
    writeFiles("parquet", dir)
    val df = spark.read.parquet(etagfs(dir)).filter(col("value") > 40)
    assert(df.count() == 9)
    assert(df.select("id", "value", "_metadata.etag").columns.toSeq == Seq("id", "value", "etag"))
  }
}
