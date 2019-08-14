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
  val defaultBufferSize: Int = 4 * 1024
  var rootBucket: String = "test-bucket" // TODO: Change temporary definition
  var confFilePath: String = "/home/shuuji3/ceph-cluster/ceph.conf"
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
    copy(src, dst)
    delete(src, recursive = false)
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
    // TODO: handle the recursive param

    val objectName = getRadosObjectName(path)
    val ioCtx = cluster.ioCtxCreate(rootBucket)
    try {
      ioCtx.remove(objectName)
      true
    } catch {
      case e: RadosNotFoundException => false
      case e: Throwable => throw new IOException
    } finally {
      ioCtx.close()
    }
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
    val radosName = getRadosObjectName(path) // i.e. dir1/object-name
    val ioCtx = cluster.ioCtxCreate(rootBucket)
    try {
      val objectNames = ioCtx.listObjects()
      objectNames.filter(_.startsWith(radosName)).map(objectName => getFileStatus(new Path(objectName)))
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
      val objectName = getRadosObjectName(path)
      val stat = ioCtx.stat(objectName)
      new FileStatus(
        stat.getSize,
        isDirectory(absolutePath),
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
   * @param f          path to create
   * @param permission to apply to f
   * @throws IOException IO failure
   */
  @throws[IOException]
  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    // TODO: implement with directory object
    true
  }

  /**
   * Copies Path src to Path dst.
   *
   * @param src path to be copied
   * @param dst new path after copy
   */
  @throws[IOException]
  private def copy(src: Path, dst: Path): Unit = {
    val readObjectName: String = getRadosObjectName(src)
    val writeObjectName: String = getRadosObjectName(dst)

    val buf = ByteBuffer.allocate(defaultBufferSize)
    val ioCtx: IoCTX = cluster.ioCtxCreate(rootBucket)
    val readChannel = new CephReadChannel(ioCtx, readObjectName, defaultBufferSize)
    val writeChannel = new CephWriteChannel(ioCtx, writeObjectName, defaultBufferSize)

    try {
      // throw RadosNotFoundException if the object does not exist
      ioCtx.stat(writeObjectName)

      // if dst file already exist stop the process
      ioCtx.close()
      throw new IOException(s"dest file '${writeObjectName}' already exists")
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
}
