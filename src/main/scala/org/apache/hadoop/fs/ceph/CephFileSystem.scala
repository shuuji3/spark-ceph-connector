package org.apache.hadoop.fs.ceph

import java.net.URI

import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.util.Progressable

class CephFileSystem extends FileSystem {
  override def getScheme: String = "ceph"

  override def getUri: URI = ???

  override def getDefaultPort: Int = 7480

  override def open(f: Path, bufferSize: Int): FSDataInputStream = ???

  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int, replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = ???

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = ???

  override def rename(src: Path, dst: Path): Boolean = ???

  override def delete(f: Path, recursive: Boolean): Boolean = ???

  override def listStatus(f: Path): Array[FileStatus] = ???

  override def getWorkingDirectory: Path = ???

  override def setWorkingDirectory(new_dir: Path): Unit = ???

  override def mkdirs(f: Path, permission: FsPermission): Boolean = ???

  override def getFileStatus(f: Path): FileStatus = ???
}
