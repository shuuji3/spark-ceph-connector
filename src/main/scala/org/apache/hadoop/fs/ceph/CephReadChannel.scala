package org.apache.hadoop.fs.ceph

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

class CephReadChannel extends SeekableByteChannel {

  override def read(dst: ByteBuffer): Int = ???

  override def write(src: ByteBuffer): Int = ???

  override def position(): Long = ???

  override def position(newPosition: Long): SeekableByteChannel = ???

  override def size(): Long = ???

  override def truncate(size: Long): SeekableByteChannel = ???

  override def isOpen: Boolean = ???

  override def close(): Unit = ???
}
