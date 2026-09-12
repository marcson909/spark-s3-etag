package com.example.spark.etag

import org.apache.spark.sql.SparkSessionExtensions

/**
 * Entry point for `spark.sql.extensions`:
 *
 * {{{
 * --conf spark.sql.extensions=com.example.spark.etag.S3EtagExtension
 * }}}
 */
class S3EtagExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit =
    extensions.injectResolutionRule(session => new EtagMetadataRule(session))
}
