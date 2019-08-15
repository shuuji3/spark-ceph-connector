package org.apache.hadoop.fs.ceph

import java.io.{FileNotFoundException, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest._
import org.scalatest.matchers._

class CephFileSystemTest extends FlatSpec with Matchers with BeforeAndAfter {

  val fs = new CephFileSystem()

  before {
    fs.setConf(new Configuration)

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

  // check for isDirectory of FileStatus
  """fs.getFileStatus(new Path("mochi-dir/")).isDirectory""" should "return true" in {
    fs.getFileStatus(new Path("mochi-dir/")).isDirectory shouldEqual true
  }

  """fs.getFileStatus(new Path("mochi-dir/mochi3")).isDirectory""" should "return false" in {
    fs.getFileStatus(new Path("mochi-dir/mochi3")).isDirectory shouldEqual false
  }

  // check for isFile of FileStatus
  """fs.getFileStatus(new Path("mochi-dir/")).isFile""" should "return false" in {
    fs.getFileStatus(new Path("mochi-dir/")).isFile shouldEqual false
  }

  """fs.getFileStatus(new Path("mochi-dir/mochi3")).isFile""" should "return true" in {
    fs.getFileStatus(new Path("mochi-dir/mochi3")).isFile shouldEqual true
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

  "listStatus('ceph://test-bucket/hello.txt')" should
    "contains FileStatus{path=ceph://test-bucket/hello.txt} and its length = 1" in {
    val statusList = fs.listStatus(new Path("ceph://test-bucket/hello.txt"))
    statusList should contain(fs.getFileStatus(new Path("hello.txt")))
    statusList.length shouldEqual 1
  }

  "listStatus('ceph://test-bucket/world.txt')" should
    "not contains FileStatus{path=ceph://test-bucket/hello.txt} and its length = 1" in {
    val statusList = fs.listStatus(new Path("ceph://test-bucket/world.txt"))
    statusList should not contain (fs.getFileStatus(new Path("hello.txt")))
    statusList.length shouldEqual 1
  }

  "listStatus('dummy-file')" should "throw FileNotFoundException" in {
    intercept[FileNotFoundException] {
      val statusList = fs.listStatus(new Path("no-exist-file"))
    }
  }

  // TODO: test directory

  // TODO: test bucket root
  //  "listStatus('ceph://test-bucket/')" should "contains FileStatus{path=ceph://test-bucket/hello.txt}" in {
  //    val statusList = fs.listStatus(new Path("ceph://test-bucket/"))
  //    statusList should contain(fs.getFileStatus(new Path("hello.txt")))
  //  }

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

  "createDirParts(Array(dir1, dir2, dir3))" should "return Array(dir1/, dir1/dir2/, dir1/dir2/dir3/)" in {
    val dirs = "dir1/dir2/dir3".split("/")
    fs.createDirParts(dirs) shouldEqual Array[String]("dir1/", "dir1/dir2/", "dir1/dir2/dir3/")
  }

  "checkIfAnyFileExists(Array(empty-dir/, mochi-dir/))" should "return false" in {
    val dirs = Array[String]("empty-dir/", "mochi-dir/")
    fs.checkIfAnyFileExists(dirs) shouldEqual false
  }

  "checkIfAnyFileExists(Array(empty-dir/, mochi-dir/mochi3))" should "return true" in {
    val dirs = Array[String]("empty-dir/", "mochi-dir/mochi3")
    fs.checkIfAnyFileExists(dirs) shouldEqual true
  }

  "checkIfAnyFileExists(Array())" should "return false" in {
    val dirs = Array[String]()
    fs.checkIfAnyFileExists(dirs) shouldEqual false
  }

  "getAllDescendantRadosObjectNames(hello.txt)" should
    "return Array(hello.txt)" in {
    fs.getAllDescendantRadosObjectNames(new Path("hello.txt")) shouldEqual Array("hello.txt")
  }

  "getAllDescendantRadosObjectNames(empty-dir)" should
    "return Array(empty-dir/)" in {
    fs.getAllDescendantRadosObjectNames(new Path("empty-dir/")) shouldEqual Array("empty-dir/")
  }

  "getAllDescendantRadosObjectNames(mochi-dir)" should
    """contains "mochi-dir/" and "mochi-dir/mochi3"""" in {
    val objectNames = fs.getAllDescendantRadosObjectNames(new Path("mochi-dir"))
    objectNames.length shouldEqual 4
    objectNames should contain("mochi-dir/")
    objectNames should contain("mochi-dir/mochi3")
  }

  "getAllDescendantRadosObjectNames(/)" should
    "return more than 7 objects (all the objects)" in {
    fs.getAllDescendantRadosObjectNames(new Path("/")).length should (be >= 7)
  }

  "getAllDescendantRadosObjectNames(no-exist-file)" should
    "return Array(no-exist-file)" in {
    intercept[FileNotFoundException] {
      fs.getAllDescendantRadosObjectNames(new Path("no-exist-file"))
    }
  }

}
