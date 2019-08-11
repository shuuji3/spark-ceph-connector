package org.apache.hadoop.fs.ceph

import java.io.{FileNotFoundException, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest._

class CephFileSystemTest extends FlatSpec with Matchers with BeforeAndAfter {

  val fs = new CephFileSystem()
  fs.setConf(new Configuration)

  "getScheme" should "return 'ceph' scheme" in {
    fs.getScheme shouldEqual "ceph"
  }

  "getFileSystemRoot" should "return 'ceph://test-bucket/'" in {
    fs.getFileSystemRoot shouldEqual new Path("ceph://test-bucket/")
  }

  "getWorkingDirectory" should "return 'ceph://test-bucket/'" in {
    fs.getWorkingDirectory shouldEqual new Path("ceph://test-bucket/")
  }

  "setWorkingDirectory('ceph://test-bucket/dir1') - absolute path" should "return 'ceph://test-bucket/dir1'" in {
    fs.setWorkingDirectory(new Path("ceph://test-bucket/dir1"))
    fs.getWorkingDirectory shouldEqual new Path("ceph://test-bucket/dir1")
  }

  "setWorkingDirectory('dir2/dir3') - relative path" should "return 'ceph://test-bucket/dir1/dir2/dir3'" in {
    fs.setWorkingDirectory(new Path("dir2/dir3"))
    fs.getWorkingDirectory shouldEqual new Path("ceph://test-bucket/dir1/dir2/dir3")
  }

  "setWorkingDirectory('..') - relative path" should "return 'ceph://test-bucket/dir1/dir2'" in {
    fs.setWorkingDirectory(new Path(".."))
    fs.getWorkingDirectory shouldEqual new Path("ceph://test-bucket/dir1/dir2")
  }

  "setWorkingDirectory('../../') - relative path" should "return 'ceph://test-bucket/'" in {
    fs.setWorkingDirectory(new Path("../../"))
    fs.getWorkingDirectory shouldEqual new Path("ceph://test-bucket/")
  }

  "getFileStatus(new Path('hello.txt')).getLen" should "return 18" in {
    fs.getFileStatus(new Path("hello.txt")).getLen shouldEqual 18
  }

  "getFileStatus(new Path('hello.txt')).getOwner" should "return ''" in {
    fs.getFileStatus(new Path("hello.txt")).getOwner shouldEqual ""
  }

  "getFileStatus(new Path('hello.txt')).getGroup" should "return ''" in {
    fs.getFileStatus(new Path("hello.txt")).getGroup shouldEqual ""
  }

  "getFileStatus(new Path('hello.txt')).getSymlink" should
    "return IOException((\"Path [ceph://test-bucket/]hello.txt is not a symbolic link\"))" in {
    val e = intercept[IOException] {
      fs.getFileStatus(new Path("hello.txt")).getSymlink
    }
    e.getMessage should endWith("hello.txt is not a symbolic link")
  }

  "getFileStatus(new Path('hello.txt')).getReplication" should "return 1" in {
    fs.getFileStatus(new Path("hello.txt")).getReplication shouldEqual 1
  }

  "getFileStatus(new Path('hello.txt')).getBlockSize" should "return 32 * 1024 * 1024" in {
    fs.getFileStatus(new Path("hello.txt")).getBlockSize shouldEqual 32 * 1024 * 1024
  }

  "getFileStatus(new Path('no-exist-file'))" should "throw FileNotFoundException" in {
    intercept[FileNotFoundException] {
      fs.getFileStatus(new Path("no-exist-file"))
    }
  }

  "getRadosObjectName(new Path('ceph://bucket-test/dir/object')" should "return dir/object" in {
    fs.getRadosObjectName(new Path("ceph://bucket-name/dir/object")) shouldEqual "dir/object"
  }

  "getRadosObjectName(new Path('dir/object')" should "return dir/object" in {
    fs.getRadosObjectName(new Path("dir/object")) shouldEqual "dir/object"
  }

  "getRadosObjectName(new Path('/dir/object')" should "return dir/object" in {
    fs.getRadosObjectName(new Path("/dir/object")) shouldEqual "dir/object"
  }

  "listStatus" should "contains FileStatus{path=ceph://test-bucket/hello.txt}" in {
    val statusList = fs.listStatus(new Path("dummy"))
    statusList should contain(fs.getFileStatus(new Path("hello.txt")))
  }
}
