package com.example.spark.etag

import scala.collection.immutable.TreeMap
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper

/** Serialises S3 user metadata as a JSON object with sorted keys, using Spark's own Jackson. */
object UserMetadataJson {
  private val mapper = new ObjectMapper()

  def encode(values: Map[String, String]): String =
    mapper.writeValueAsString(TreeMap.from(values).asJava)
}
