package com.example.spark.etag

import org.scalatest.funsuite.AnyFunSuite

class UserMetadataJsonSuite extends AnyFunSuite {

  test("encodes keys in sorted order with JSON string escaping") {
    val json = UserMetadataJson.encode(Map("name" -> "a \"quoted\" \\ name", "mtime" -> "1694500000.123456789"))
    assert(json == """{"mtime":"1694500000.123456789","name":"a \"quoted\" \\ name"}""")
  }

  test("encodes an empty map as an empty object") {
    assert(UserMetadataJson.encode(Map.empty) == "{}")
  }
}
