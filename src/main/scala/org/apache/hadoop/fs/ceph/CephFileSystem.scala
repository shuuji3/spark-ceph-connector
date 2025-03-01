package org.apache.hadoop.fs.ceph

import java.io.{File, FileNotFoundException, IOException}
import java.net.URI
import java.nio.ByteBuffer

import com.ceph.rados.exceptions.RadosNotFoundException
import com.ceph.rados.{IoCTX, Rados}
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

class CephFileSystem extends FileSystem {
  // Connect to a Ceph cluster
  val cluster: Rados = new Rados("admin")
  val defaultBufferSize: Int = 4 * 1024 * 1024 // 4 MiB
  var rootBucket: String = "test-bucket" // TODO: Change temporary definition
  var confFilePath: String = "/etc/ceph/ceph.conf"
  cluster.confReadFile(new File(confFilePath))
  cluster.connect()
  var workingDirectory: Path = getFileSystemRoot

  /**
   * Returns a URI which identifies this FileSystem.
   *
   * @return the URI of this filesystem.
   */
  override def getUri: URI = getFileSystemRoot.toUri

  def getFileSystemRoot: Path = new Path(getScheme + "://" + rootBucket + "/")

  override def getScheme: String = "ceph"

  /**
   * Opens an FSDataInputStream at the indicated Path.
   *
   * @param path       the file name to open
   * @param bufferSize the size of the buffer to be used.
   * @throws IOException IO failure
   */
  @throws[IOException]
  override def open(path: Path, bufferSize: Int): FSDataInputStream = {
    val ioCtx: IoCTX = cluster.ioCtxCreate(rootBucket)
    val objectName: String = getRadosObjectName(path)
    val in = new CephFSInputStream(ioCtx, objectName, bufferSize)
    new FSDataInputStream(in)
  }

  /**
   * Create a Rados object name from Path
   *
   * @param path relative or absolute path i.e. <code>Path("/dir/object-name")</code>
   * @return rados object name i.e. <code>dir/object-name</code>
   */
  def getRadosObjectName(path: Path): String = {
    val objectPath: String = fixRelativePart(path).toUri.getPath
    "^/".r.replaceFirstIn(objectPath, "") // Remove prefix '/'
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   *
   * @param path        the file name to open
   * @param permission  file permission
   * @param overwrite   if a file with this name already exists, then if true,
   *                    the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize  the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize   block size
   * @param progress    the progress reporter
   * @throws IOException IO failure
   * @see #setPermission(Path, FsPermission)
   */
  @throws[IOException]
  override def create(path: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int, replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = {
    val ioCtx: IoCTX = cluster.ioCtxCreate(rootBucket)
    val objectName = getRadosObjectName(path)
    if (isDirectory(path)) {
      throw new FileAlreadyExistsException(s"${path} is a directory")
    } else if (isFile(path) && !overwrite) {
      throw new FileAlreadyExistsException(s"${path} is already exists and specified not to overwrite")
    }

    // Make parent dirs
    val parentPath = path.getParent
    mkdirs(parentPath)

    // Touch when create()
    ioCtx.write(objectName, "")

    val out = new CephFSDataOutputStream(ioCtx, objectName, bufferSize)
    new FSDataOutputStream(out, null)
  }

  /**
   * Append to an existing file (optional operation).
   *
   * @param f          the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @param progress   for reporting progress if it is not null.
   * @throws IOException                   IO failure
   * @throws UnsupportedOperationException if the operation is unsupported
   *                                       (default).
   */
  @throws[IOException]
  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = {
    throw new UnsupportedOperationException("CephFileSystem: not supported")
  }

  /**
   * Renames Path src to Path dst.
   *
   * @param src path to be renamed
   * @param dst new path after rename
   * @return true if rename is successful
   * @throws IOException on failure
   */
  @throws[IOException]
  override def rename(src: Path, dst: Path): Boolean = {
    // Check if dst exists
    if (exists(dst)) {
      throw new FileAlreadyExistsException(s"${dst} already exists")
    }

    if (isFile(src)) {
      val srcObjectName: String = getRadosObjectName(src)
      val dstObjectName: String = getRadosObjectName(dst)
      radosCopy(srcObjectName, dstObjectName)
      delete(src, recursive = false)
    } else if (isDirectory(src)) {
      val directoryName = s"${getRadosObjectName(src)}/"
      val newDirectoryName = s"${getRadosObjectName(dst)}/"

      val objectNames: Array[String] = getAllDescendantRadosObjectNames(src)
      objectNames.foreach(objectName => {
        val newObjectName = s"^${directoryName}".r.replaceFirstIn(objectName, newDirectoryName)
        radosCopy(objectName, newObjectName)
      })
      delete(src, recursive = true)
    } else {
      throw new IOException(s"${src} is neither a file nor a directory")
    }
  }

  /**
   * Delete a file.
   *
   * @param path      the path to delete.
   * @param recursive if path is a directory and set to
   *                  true, the directory is deleted else throws an exception. In
   *                  case of a file the recursive can be set to either true or false.
   * @return true if delete is successful else false.
   * @throws IOException IO failure
   */
  @throws[IOException]
  override def delete(path: Path, recursive: Boolean): Boolean = {
    if (isFile(path)) {
      val objectName: String = getRadosObjectName(path)
      radosDelete(objectName)
    } else if (isDirectory(path)) {
      if (recursive) {
        val objectNames = getAllDescendantRadosObjectNames(path)
        objectNames.foreach(radosDelete)
        true
      } else {
        throw new IOException(s"${path} did not deleted because recursive is set to false")
      }
    } else {
      // Return false not FileNotFoundException When the object not found
      false
    }
  }

  /**
   * Delete an object using the object name.
   *
   * This methods is resemble to the delete() but the parameter is String type
   * to handle RADOS directory object (i.e. dir-name/ not dir-name)
   *
   * @param objectName the object to delete
   * @return
   */
  @throws[IOException]
  def radosDelete(objectName: String): Boolean = {
    val ioCtx = cluster.ioCtxCreate(rootBucket)
    try {
      ioCtx.remove(objectName)
      true
    } catch {
      case _: RadosNotFoundException => false
    } finally {
      ioCtx.close()
    }
  }

  /**
   * Copy RADOS object with new name.
   *
   * @param readObjectName  objectName to be copied
   * @param writeObjectName new objectName created
   */
  @throws[IOException]
  def radosCopy(readObjectName: String, writeObjectName: String): Unit = {
    val buf = ByteBuffer.allocate(defaultBufferSize)
    val ioCtx: IoCTX = cluster.ioCtxCreate(rootBucket)
    val readChannel = new CephReadChannel(ioCtx, readObjectName, defaultBufferSize)
    val writeChannel = new CephWriteChannel(ioCtx, writeObjectName, defaultBufferSize)

    try {
      // throw RadosNotFoundException if the object does not exist
      ioCtx.stat(writeObjectName)

      // if dst file already exist stop the process
      ioCtx.close()
      throw new FileAlreadyExistsException(s"dest file '${writeObjectName}' already exists")
    } catch {
      case _: RadosNotFoundException => ()
    }

    try {
      var numRead = -1
      while (numRead != 0) {
        numRead = readChannel.read(buf)
        buf.flip
        writeChannel.write(buf)
        buf.clear
      }
    } finally {
      ioCtx.close()
    }
  }

  /**
   * True iff the named path is a directory.
   * Note: Avoid using this method. Instead reuse the FileStatus
   * returned by getFileStatus() or listStatus() methods.
   *
   * @param path path to check
   * @throws IOException IO failure
   */
  @throws[IOException]
  override def isDirectory(path: Path): Boolean = {
    val ioCtx = cluster.ioCtxCreate(rootBucket)
    val objectName: String = getRadosObjectName(path)
    val directoryName: String = s"${objectName}/"

    try {
      ioCtx.stat(directoryName) // throw RadosNotFoundException if not exist
      true
    } catch {
      case _: RadosNotFoundException => false
    } finally {
      ioCtx.close()
    }
  }

  /** True iff the named path is a regular file.
   * Note: Avoid using this method. Instead reuse the FileStatus
   * returned by {@link #getFileStatus(Path)} or listStatus() methods.
   *
   * @param path path to check
   * @throws IOException IO failure
   */
  @throws[IOException]
  override def isFile(path: Path): Boolean = {
    val ioCtx = cluster.ioCtxCreate(rootBucket)
    val objectName: String = getRadosObjectName(path)

    if (objectName == "") {
      // In case objectName is empty (root path), ioCtx.stat() does not throw RadosNotFoundException
      return false
    }

    try {
      ioCtx.stat(objectName) // throw RadosNotFoundException if not exist
      true
    } catch {
      case e: RadosNotFoundException => false
      case _: Throwable => {
        println("[error]   * caught unknown Throwable") // debug
        false
      }
    } finally {
      ioCtx.close()
    }
  }

  /**
   * Check if the given path is the bucket root
   *
   * @param path the given path
   * @return true if the path is bucket root
   */
  def isBucketRoot(path: Path): Boolean = {
    path == new Path("/") ||
      path == new Path("ceph://test-bucket/") // note: not "ceph://test-bucket"
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * <p>
   * Does not guarantee to return the List of files/directories status in a
   * sorted order.
   *
   * @param path given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException           see specific implementation
   */
  @throws[FileNotFoundException]
  @throws[IOException]
  override def listStatus(path: Path): Array[FileStatus] = {
    val ioCtx = cluster.ioCtxCreate(rootBucket)
    try {
      if (isFile(path)) {
        Array[FileStatus](getFileStatus(path))
      } else if (isDirectory(path) || isBucketRoot(path)) {
        val allDescendantObjectNames =
          getAllDescendantRadosObjectNames(path, limitCurrentDir = true, excludeGivenDirectory = true)
        allDescendantObjectNames.map(objectName => getFileStatus(new Path(objectName)))
      } else {
        throw new FileNotFoundException(s"listStatus: ${path} not found")
      }
    } finally {
      ioCtx.close()
    }
  }

  /**
   * Return a file status object that represents the path.
   *
   * @param path The path we want information from
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException           see specific implementation
   */
  @throws[IOException]
  override def getFileStatus(path: Path): FileStatus = {
    val ioCtx = cluster.ioCtxCreate(rootBucket)
    try {
      val absolutePath: Path = fixRelativePart(path)
      // TODO: should return FileStatus for the root bucket?
      val isDir: Boolean = isDirectory(absolutePath)
      val objectName: String = getRadosObjectName(path)
      val fileName: String = if (isDir) objectName + "/" else objectName
      val stat = ioCtx.stat(fileName)
      new FileStatus(
        stat.getSize,
        isDir,
        getDefaultReplication(absolutePath),
        getDefaultBlockSize(absolutePath),
        stat.getMtime,
        0,
        null,
        null,
        null,
        absolutePath
      )
    } catch {
      case e: RadosNotFoundException => throw new FileNotFoundException(e.getMessage)
      case e: Throwable =>
        throw new IOException(e)
    } finally {
      ioCtx.close()
    }
  }

  /**
   * Get the current working directory for the given FileSystem
   *
   * @return the directory pathname
   */
  override def getWorkingDirectory: Path = {
    workingDirectory
  }

  /**
   * Set the current working directory for the given FileSystem. All relative
   * paths will be resolved relative to it.
   *
   * @param new_dir Path of new working directory
   */
  override def setWorkingDirectory(new_dir: Path): Unit = {
    val path = fixRelativePart(new_dir)
    workingDirectory = path
  }

  /**
   * Make the given file and all non-existent parents into
   * directories. Has roughly the semantics of Unix @{code mkdir -p}.
   * Existence of the directory hierarchy is not an error.
   *
   * @param path       path to create
   * @param permission to apply to f
   * @throws IOException IO failure
   */
  @throws[IOException]
  override def mkdirs(path: Path, permission: FsPermission): Boolean = {
    val directoryName = getRadosObjectName(path)
    val dirParts = createDirParts(directoryName.split("/"))
    if (checkIfAnyFileExists(dirParts)) {
      false
    } else {
      val ioCtx: IoCTX = cluster.ioCtxCreate(rootBucket)
      dirParts.foreach(dirPart => mkdir(ioCtx, dirPart))
      true
    }
  }

  /**
   * Make the given file and all non-existent parents into
   * directories. Has roughly the semantics of Unix @{code mkdir -p}.
   * Existence of the directory hierarchy is not an error.
   *
   * @param ioCtx   connection to the pool
   * @param dirName directory name to mkdir i.e. dir1/dir2/
   * @throws IOException IO failure
   */
  @throws[IOException]
  def mkdir(ioCtx: IoCTX, dirName: String): Unit = {
    ioCtx.write(dirName, "")
  }

  /**
   * Make directory parts of all the descending directories for <code>mkdirs()</code>.
   *
   * When Array(dir1, dir2, dir3) is given, returns Array(dir1, dir1/dir2, dir1/dir2/dir3)
   *
   * @param dirs directory name lists to make
   * @return
   */
  def createDirParts(dirs: Array[String]): Array[String] = {
    if (dirs.isEmpty) {
      new Array[String](0)
    } else {
      createDirParts(dirs.init) :+ dirs.mkString("", "/", "/")
    }
  }

  /**
   * Check if not existing any file in dirs to create directory.
   * If dirParts is empty, return false.
   *
   * i.e.
   * Array(dir1/, dir1/dir2/) => false
   * Array(dir1/, dir1/object1) => true
   * Array() => false
   *
   * @param dirParts directory list to check
   * @return true if there is any file in all the path in dirParts
   */
  def checkIfAnyFileExists(dirParts: Array[String]): Boolean = {
    dirParts.exists(dirPart => isFile(new Path(dirPart)))
  }

  /**
   * Get all the RADOS object names which is a descendant of the given path.
   * if the given path is file, return an array of single string
   *
   * i.e.
   * (dir) => Array(dir/, dir/object1, dir/object2)
   * (object3) => Array(object3)
   *
   * @param path root object path
   * @return descendant object names
   */
  def getAllDescendantRadosObjectNames(path: Path, limitCurrentDir: Boolean = false,
                                       excludeGivenDirectory: Boolean = false): Array[String] = {
    val ioCtx: IoCTX = cluster.ioCtxCreate(rootBucket)
    val pathIsBucketRoot: Boolean = isBucketRoot(path)
    try {
      if (isFile(path)) {
        Array[String](getRadosObjectName(path))
      } else if (isDirectory(path) || pathIsBucketRoot) {
        val ioCtx: IoCTX = cluster.ioCtxCreate(rootBucket)
        val directoryName = if (pathIsBucketRoot) "" else s"${getRadosObjectName(path)}/"
        val isDescendantRegex = if (limitCurrentDir)
          s"${directoryName}[^/]*/?$$".r else s"${directoryName}[^/]*/?".r

        val allObjectNames: Array[String] = ioCtx.listObjects()
        val currentDirObjects =
          allObjectNames
            .filter(objectName => isDescendantRegex.findPrefixMatchOf(objectName).isDefined)
        if (excludeGivenDirectory) {
          currentDirObjects.filter(_ != directoryName)
        } else {
          currentDirObjects
        }
      } else {
        throw new FileNotFoundException(s"${path} not exists")
      }
    } finally {
      ioCtx.close()
    }
  }
}
