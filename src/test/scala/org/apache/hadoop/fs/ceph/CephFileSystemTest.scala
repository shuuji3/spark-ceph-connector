package org.apache.hadoop.fs.ceph

import org.apache.hadoop.fs.Path
import org.scalatest._

class CephFileSystemTest extends FlatSpec with Matchers with BeforeAndAfter {

  val fs = new CephFileSystem()

  "getScheme" should "return 'ceph' scheme" in {
    fs.getScheme shouldEqual "ceph"
  }

  "getFileSystemRoot" should "return 'ceph://test-bucket/'" in {
    fs.getFileSystemRoot shouldEqual new Path("ceph://test-bucket/")
  }

  "getWorkingDirectory" should "return 'ceph://test-bucket/'" in {
    fs.getWorkingDirectory shouldEqual new Path("ceph://test-bucket/")
  }

  "setWorkingDirectory(<absolute path>)" should "return 'ceph://test-bucket/dir1'" in {
    fs.setWorkingDirectory(new Path("ceph://test-bucket/dir1"))
    fs.getWorkingDirectory shouldEqual new Path("ceph://test-bucket/dir1")
  }

  "setWorkingDirectory('dir1/dir2') - relative path" should "return 'ceph://test-bucket/dir1/dir2'" in {
    fs.setWorkingDirectory(new Path("dir1/dir2"))
    fs.getWorkingDirectory shouldEqual new Path("ceph://test-bucket/dir1/dir2")
  }
}
