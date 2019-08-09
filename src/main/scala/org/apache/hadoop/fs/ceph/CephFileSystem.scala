package org.apache.hadoop.fs.ceph

import java.io.{File, FileNotFoundException, IOException}
import java.net.URI

import com.ceph.rados.Rados
import com.ceph.rados.exceptions.RadosNotFoundException
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

class CephFileSystem extends FileSystem {
  // Connect to a Ceph cluster
  val cluster: Rados = new Rados("admin")
  var rootBucket: String = "test-bucket" // TODO: Change temporary definition
  var confFilePath: String = "/home/shuuji3/ceph-cluster/ceph.conf"
  var workingDirectory: Path = getFileSystemRoot
  cluster.confReadFile(new File(confFilePath))
  cluster.connect()

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
   * @param f          the file name to open
   * @param bufferSize the size of the buffer to be used.
   * @throws IOException IO failure
   */
  @throws[IOException]
  override def open(f: Path, bufferSize: Int): FSDataInputStream = null

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   *
   * @param f           the file name to open
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
  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int, replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = null

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
  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = null

  /**
   * Renames Path src to Path dst.
   *
   * @param src path to be renamed
   * @param dst new path after rename
   * @return true if rename is successful
   * @throws IOException on failure
   */
  @throws[IOException]
  override def rename(src: Path, dst: Path): Boolean = false

  /**
   * Delete a file.
   *
   * @param f         the path to delete.
   * @param recursive if path is a directory and set to
   *                  true, the directory is deleted else throws an exception. In
   *                  case of a file the recursive can be set to either true or false.
   * @return true if delete is successful else false.
   * @throws IOException IO failure
   */
  @throws[IOException]
  override def delete(f: Path, recursive: Boolean): Boolean = {
    // TODO: handle the recursive param
    val ctx = cluster.ioCtxCreate(rootBucket)
    try {
      ctx.remove("hello.txt") // TODO: change temp oid
      true
    } catch {
      case e: RadosNotFoundException => false
      case e: Throwable => throw new IOException
    } finally {
      ctx.close()
    }
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * <p>
   * Does not guarantee to return the List of files/directories status in a
   * sorted order.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException           see specific implementation
   */
  @throws[FileNotFoundException]
  @throws[IOException]
  override def listStatus(f: Path): Array[FileStatus] = new Array[FileStatus](0)

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
  override def mkdirs(f: Path, permission: FsPermission): Boolean = false

  /**
   * Return a file status object that represents the path.
   *
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException           see specific implementation
   */
  @throws[IOException]
  override def getFileStatus(path: Path): FileStatus = {
    val ctx = cluster.ioCtxCreate(rootBucket)
    try {
      val stat = ctx.stat("hello.txt") // TODO: change temp oid
      new FileStatus(
        stat.getSize,
        false,
        getDefaultReplication(path),
        getDefaultBlockSize(path),
        stat.getMtime,
        0,
        null,
        null,
        null,
        path
      )
    } catch {
      case e: RadosNotFoundException => throw new FileNotFoundException
      case e: Throwable => throw new IOException
    } finally {
      ctx.close()
    }
  }
}
