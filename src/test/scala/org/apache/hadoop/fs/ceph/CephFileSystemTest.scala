package org.apache.hadoop.fs.ceph

import java.io.{FileNotFoundException, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest._

class CephFileSystemTest extends FlatSpec with Matchers with BeforeAndAfter {

  val fs = new CephFileSystem()

  before {
    fs.setConf(new Configuration)

    // Prepare 'hello.txt' for test
    fs.delete(new Path("ceph://test-bucket/hello.txt"), recursive = false)
    val hello = fs.create(new Path("ceph://test-bucket/hello.txt"))
    hello.write("hello scala world!\n".map(_.toByte).toArray)

    // val out = fs.create(new Path("ceph://test-bucket/mochi"))
    // out.write("mochi".map(_.toByte).toArray)
  }

  after {
    // fs.delete(new Path("ceph://test-bucket/hello.txt"), recursive = false)
    // fs.delete(new Path("ceph://test-bucket/mochi"), false)
  }

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

  "getFileStatus(new Path('hello.txt')).getLen" should "return 19" in {
    fs.getFileStatus(new Path("hello.txt")).getLen shouldEqual 19
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

  //  "listStatus('ceph://test-bucket/')" should "contains FileStatus{path=ceph://test-bucket/hello.txt}" in {
  //    val statusList = fs.listStatus(new Path("ceph://test-bucket/"))
  //    statusList should contain(fs.getFileStatus(new Path("hello.txt")))
  //  }

  "listStatus('ceph://test-bucket/hello.txt')" should "contains FileStatus{path=ceph://test-bucket/hello.txt}" in {
    val statusList = fs.listStatus(new Path("ceph://test-bucket/hello.txt"))
    statusList should contain(fs.getFileStatus(new Path("hello.txt")))
  }

  "listStatus('ceph://test-bucket/world.txt')" should "not contains FileStatus{path=ceph://test-bucket/hello.txt}" in {
    val statusList = fs.listStatus(new Path("ceph://test-bucket/world.txt"))
    statusList should not contain (fs.getFileStatus(new Path("hello.txt")))
  }

  "listStatus('dummy-file')" should "not contains any FileStatus" in {
    val statusList = fs.listStatus(new Path("dummy-file"))
    statusList.length shouldEqual 0
  }

  "open(new Path(\"hello.txt\"))" should "read string '**hello sc**ala world!' when 8 byte read" in {
    val inputStream = fs.open(new Path("hello.txt"))
    val buf = new Array[Byte](8)

    val numRead = inputStream.read(buf)
    numRead shouldEqual 8
    new String(buf.array) shouldEqual "hello sc"
  }

  "open(new Path(\"hello.txt\"))" should "read string 'hello sc**ala worl**d!' when 8 byte read" in {
    val inputStream = fs.open(new Path("hello.txt"))
    inputStream.seek(8)
    val buf = new Array[Byte](8)

    val numRead = inputStream.read(buf)
    numRead shouldEqual 8
    new String(buf.array) shouldEqual "ala worl"
  }

  "open(new Path(\"hello.txt\"))" should "read string 'hello scala worl**d!**' when 8 byte read" in {
    val inputStream = fs.open(new Path("hello.txt"))
    inputStream.seek(16)
    val buf = new Array[Byte](8)

    val numRead = inputStream.read(buf)
    numRead shouldEqual 3
    new String(buf.array) shouldEqual "d!\n\0\0\0\0\0"
  }

  "open(new Path(\"hello.txt\"))" should "read string 'hello scala world!'" in {
    val inputStream = fs.open(new Path("hello.txt"))
    inputStream.seek(0)
    val buf = new Array[Byte](8)

    var totalRead = 0
    var readString = ""
    var numRead = inputStream.read(buf)
    totalRead += numRead
    readString += new String(buf.array.slice(0, numRead))
    numRead shouldEqual 8
    new String(buf.array) shouldEqual "hello sc"

    numRead = inputStream.read(buf)
    totalRead += numRead
    readString += new String(buf.array.slice(0, numRead))
    numRead shouldEqual 8
    new String(buf.array) shouldEqual "ala worl"

    numRead = inputStream.read(buf)
    totalRead += numRead
    readString += new String(buf.array.slice(0, numRead))
    numRead shouldEqual 3
    new String(buf.array.slice(0, numRead)) shouldEqual "d!\n"

    totalRead shouldEqual 19
    readString shouldEqual "hello scala world!\n"
  }

  """isDirectory(new Path("empty-dir/"))""" should "return true" in {
    fs.isDirectory(new Path("empty-dir/")) shouldEqual true
  }

  """isDirectory(new Path("mochi-dir/"))""" should "return true" in {
    fs.isDirectory(new Path("mochi-dir/")) shouldEqual true
  }

  """isDirectory(new Path("not-exist-dir/"))""" should "return false" in {
    fs.isDirectory(new Path("not-exist-dir/")) shouldEqual false
  }

  """isDirectory(new Path("mochi-dir/mochi1"))""" should "return false" in {
    fs.isDirectory(new Path("mochi-dir/mochi1")) shouldEqual false
  }

  """isFile(new Path("mochi-dir/mochi1"))""" should "return true" in {
    fs.isFile(new Path("mochi-dir/mochi1")) shouldEqual true
  }

  """isFile(new Path("mochi-dir/"))""" should "return false" in {
    fs.isFile(new Path("mochi-dir/")) shouldEqual false
  }

  """isFile(new Path("not-exist-file"))""" should "return false" in {
    fs.isFile(new Path("not-exist-file")) shouldEqual false
  }
}
