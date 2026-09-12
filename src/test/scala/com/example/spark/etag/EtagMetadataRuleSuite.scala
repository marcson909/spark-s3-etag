package com.example.spark.etag

import java.nio.file.Files

import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite

/** Exercises the analyzer rule directly, on relations it has not rewritten yet. */
class EtagMetadataRuleSuite extends AnyFunSuite with EtagSparkSession {

  // spark is null until beforeAll, so build the rule inside each test.
  private def rule: EtagMetadataRule = new EtagMetadataRule(spark)

  /** A LogicalRelation over an etagfs path that the rule has not seen, read with the rewrite off. */
  private def plainRelation(): LogicalRelation = {
    val dir = Files.createTempDirectory("rule-suite").resolve("data").toFile
    spark.range(0, 5).write.parquet(dir.getAbsolutePath)
    withConf(EtagMetadataRule.EnabledKey, "false") {
      spark.read.parquet("etagfs://" + dir.getAbsolutePath).queryExecution.analyzed
        .collectFirst { case l: LogicalRelation => l }.get
    }
  }

  private def locationOf(plan: Any): Any =
    plan.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location

  // resolveOperatorsUp skips nodes already marked analyzed, so every test copies first.
  test("rewrites a fresh relation") {
    val rewritten = rule(plainRelation().copy())
    val fsRelation = rewritten.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation]
    assert(fsRelation.location.isInstanceOf[EtagFileIndex])
    assert(fsRelation.fileFormat.isInstanceOf[EtagParquetFileFormat])
  }

  test("leaves a streaming relation unchanged") {
    val streaming = plainRelation().copy(isStreaming = true)
    assert(rule(streaming) eq streaming)
  }

  test("is idempotent") {
    val once = rule(plainRelation().copy()).asInstanceOf[LogicalRelation]
    val twice = rule(once.copy())
    val index = locationOf(twice).asInstanceOf[EtagFileIndex]
    assert(!index.delegate.isInstanceOf[EtagFileIndex])
    assert(index eq locationOf(once).asInstanceOf[EtagFileIndex])
  }

  test("re-adds an existing _metadata attribute with the wider struct") {
    val withMeta = plainRelation().withMetadataColumns().asInstanceOf[LogicalRelation].copy()
    val rewritten = rule(withMeta)
    val metadata = rewritten.output.filter(_.name == "_metadata")
    assert(metadata.size == 1)
    val fields = metadata.head.dataType.asInstanceOf[StructType].fieldNames.toSeq
    assert(fields.contains("etag"))
    assert(fields.contains("user_metadata"))
  }
}
