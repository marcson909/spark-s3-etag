package com.example.spark.etag

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{EtagSource, Path}
import org.scalatest.funsuite.AnyFunSuite

class EtagLocalFileSystemSuite extends AnyFunSuite {

  private val abcMd5 = "900150983cd24fb0d6963f7d28e17f72" // RFC 1321 test vector for "abc"

  test("md5Hex matches the RFC 1321 test vector") {
    assert(EtagLocalFileSystem.md5Hex("abc".getBytes(StandardCharsets.US_ASCII)) == abcMd5)
  }

  test("listStatus and getFileStatus return statuses carrying the MD5 as etag") {
    val dir = Files.createTempDirectory("etagfs-suite").toFile
    Files.write(new File(dir, "a.txt").toPath, "abc".getBytes(StandardCharsets.US_ASCII))
    val conf = new Configuration()
    conf.set("fs.etagfs.impl", classOf[EtagLocalFileSystem].getName)
    val dirPath = new Path("etagfs://" + dir.getAbsolutePath)
    val fs = dirPath.getFileSystem(conf)

    val listed = fs.listStatus(dirPath)
    assert(listed.length == 1)
    assert(listed.head.getPath.toUri.getScheme == "etagfs")
    assert(listed.head.asInstanceOf[EtagSource].getEtag == abcMd5)

    val single = fs.getFileStatus(new Path(dirPath, "a.txt"))
    assert(single.asInstanceOf[EtagSource].getEtag == abcMd5)

    assert(!fs.getFileStatus(dirPath).isInstanceOf[EtagSource], "directories carry no etag")
  }

  test("listStatusCalls counts every listStatus call") {
    val dir = Files.createTempDirectory("etagfs-count").toFile
    val conf = new Configuration()
    conf.set("fs.etagfs.impl", classOf[EtagLocalFileSystem].getName)
    val dirPath = new Path("etagfs://" + dir.getAbsolutePath)
    val fs = dirPath.getFileSystem(conf)
    val before = EtagLocalFileSystem.listStatusCalls.get()
    fs.listStatus(dirPath)
    fs.listStatus(dirPath)
    assert(EtagLocalFileSystem.listStatusCalls.get() == before + 2)
  }
}
