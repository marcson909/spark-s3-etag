package com.example.spark.etag

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.FileSourceMetadataAttribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
 * Rewrites file relations so their `_metadata` struct gains an `etag` field.
 *
 * Injected as a resolution rule, it runs in the same analyzer iteration that turns a path or
 * table into a LogicalRelation and before `_metadata` references are resolved against it. A
 * relation is rewritten when its format is one of Spark's built-in Parquet/ORC/CSV/JSON formats
 * and every root path uses a scheme listed in `spark.sql.s3etag.schemes`.
 */
class EtagMetadataRule(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan =
    if (!enabled) plan
    else plan.resolveOperatorsUp {
      case relation: LogicalRelation =>
        relation.relation match {
          case fsRelation: HadoopFsRelation if shouldRewrite(fsRelation) => rewrite(relation, fsRelation)
          case _ => relation
        }
    }

  /** A boolean session config; a value that is not a boolean logs a warning and means the default. */
  private def booleanConf(key: String, default: Boolean): Boolean = {
    val raw = session.conf.get(key, default.toString).trim
    raw.toLowerCase(java.util.Locale.ROOT) match {
      case "true" => true
      case "false" => false
      case other =>
        logWarning(s"Ignoring value '$other' of $key because it is not a boolean; using $default")
        default
    }
  }

  private def enabled: Boolean = booleanConf(EtagMetadataRule.EnabledKey, default = true)

  private def enabledSchemes: Set[String] =
    session.conf.get(EtagMetadataRule.SchemesKey, EtagMetadataRule.DefaultSchemes)
      .split(",").map(_.trim.toLowerCase).filter(_.nonEmpty).toSet

  private def shouldRewrite(fsRelation: HadoopFsRelation): Boolean = {
    val schemes = enabledSchemes
    val roots = fsRelation.location.rootPaths
    !fsRelation.location.isInstanceOf[EtagFileIndex] &&
      EtagFileFormats.replacementFor(fsRelation.fileFormat).isDefined &&
      roots.nonEmpty &&
      roots.forall(root => schemes.contains(schemeOf(root)))
  }

  private def schemeOf(path: Path): String =
    Option(path.toUri.getScheme).getOrElse("").toLowerCase

  private def rewrite(relation: LogicalRelation, fsRelation: HadoopFsRelation): LogicalRelation = {
    val rewrittenFsRelation = fsRelation.copy(
      location = new EtagFileIndex(fsRelation.location, session.sessionState.newHadoopConf()),
      fileFormat = EtagFileFormats.replacementFor(fsRelation.fileFormat).get)(fsRelation.sparkSession)

    // If Spark already added a _metadata attribute to this relation's output, its struct type
    // lacks etag. Drop it and let the new relation add its own, whose type includes etag.
    val (metadataAttributes, dataAttributes) =
      relation.output.partition(attr => FileSourceMetadataAttribute.unapply(attr).isDefined)
    val rewritten = relation.copy(relation = rewrittenFsRelation, output = dataAttributes)
    rewritten.copyTagsFrom(relation)
    if (metadataAttributes.isEmpty) rewritten else rewritten.withMetadataColumns()
  }
}

object EtagMetadataRule {
  val EnabledKey: String = "spark.sql.s3etag.enabled"
  val SchemesKey: String = "spark.sql.s3etag.schemes"
  val DefaultSchemes: String = "s3a"
}
