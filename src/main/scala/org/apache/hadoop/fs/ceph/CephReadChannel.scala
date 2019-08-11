package org.apache.hadoop.fs.ceph

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.{ClosedChannelException, NonWritableChannelException, ReadableByteChannel, SeekableByteChannel}

import com.ceph.rados.IoCTX

class CephReadChannel(ioCtx: IoCTX, objectName: String, bufferSize: Int) extends SeekableByteChannel {
  // TDOO: Implement constructor
  // ByteBuffer.allocate(bufferSize)

  var channelIsOpen: Boolean = true
  var channelPosition: Long = 0
  var channel: ReadableByteChannel = _
  val objectSize: Long = ioCtx.stat(objectName).getSize

  /**
   * Reads a sequence of bytes from this channel into the given buffer.
   *
   * <p> Bytes are read starting at this channel's current position, and
   * then the position is updated with the number of bytes actually read.
   * Otherwise this method behaves exactly as specified in the {@link
   * ReadableByteChannel} interface.
   */
  @throws[IOException]
  override def read(dst: ByteBuffer): Int = {
    // TODO: copy data of RADOS object to dst (ByteBuffer) by using librados
    // val nRead = ioCtx.read(objectName, len, offset, buf)
    0
  }

  /**
   * Writes a sequence of bytes to this channel from the given buffer.
   *
   * <p> Bytes are written starting at this channel's current position, unless
   * the channel is connected to an entity such as a file that is opened with
   * the {@link java.nio.file.StandardOpenOption#APPEND APPEND} option, in
   * which case the position is first advanced to the end. The entity to which
   * the channel is connected is grown, if necessary, to accommodate the
   * written bytes, and then the position is updated with the number of bytes
   * actually written. Otherwise this method behaves exactly as specified by
   * the {@link WritableByteChannel} interface.
   */
  @throws[IOException]
  override def write(src: ByteBuffer): Int = {
    throw new IOException("CephReadChannel: cannot change a read-only channel")
  }

  /**
   * Returns this channel's position.
   *
   * @return This channel's position,
   *         a non-negative integer counting the number of bytes
   *         from the beginning of the entity to the current position
   * @throws  ClosedChannelException
   * If this channel is closed
   * @throws  IOException
   * If some other I/O error occurs
   */
  @throws[IOException]
  def position: Long = {
    checkIsOpen()
    channelPosition
  }

  /**
   * Sets this channel's position.
   *
   * <p> Setting the position to a value that is greater than the current size
   * is legal but does not change the size of the entity.  A later attempt to
   * read bytes at such a position will immediately return an end-of-file
   * indication.  A later attempt to write bytes at such a position will cause
   * the entity to grow to accommodate the new bytes; the values of any bytes
   * between the previous end-of-file and the newly-written bytes are
   * unspecified.
   *
   * <p> Setting the channel's position is not recommended when connected to
   * an entity, typically a file, that is opened with the {@link
   * java.nio.file.StandardOpenOption#APPEND APPEND} option. When opened for
   * append, the position is first advanced to the end before writing.
   *
   * @param  newPosition
   * The new position, a non-negative integer counting
   * the number of bytes from the beginning of the entity
   * @return This channel
   * @throws  ClosedChannelException
   * If this channel is closed
   * @throws  IllegalArgumentException
   * If the new position is negative
   * @throws  IOException
   * If some other I/O error occurs
   */
  @throws[IOException]
  def position(newPosition: Long): SeekableByteChannel = {
    checkIsOpen()
    channelPosition = newPosition
    this
  }

  /**
   * Returns the current size of entity to which this channel is connected.
   *
   * @return The current size, measured in bytes
   * @throws  ClosedChannelException
   * If this channel is closed
   * @throws  IOException
   * If some other I/O error occurs
   */
  @throws[IOException]
  def size: Long = {
    checkIsOpen()
    objectSize
  }

  /**
   * Check the channel is open
   */
  @throws[IOException]
  private def checkIsOpen(): Unit = {
    if (!isOpen) throw new ClosedChannelException
  }

  /**
   * Tells whether or not this channel is open.
   *
   * @return <tt>true</tt> if, and only if, this channel is open
   */
  def isOpen: Boolean = channelIsOpen

  /**
   * Truncates the entity, to which this channel is connected, to the given
   * size.
   *
   * <p> If the given size is less than the current size then the entity is
   * truncated, discarding any bytes beyond the new end. If the given size is
   * greater than or equal to the current size then the entity is not modified.
   * In either case, if the current position is greater than the given size
   * then it is set to that size.
   *
   * <p> An implementation of this interface may prohibit truncation when
   * connected to an entity, typically a file, opened with the {@link
   * java.nio.file.StandardOpenOption#APPEND APPEND} option.
   *
   * @param  size
   * The new size, a non-negative byte count
   * @return This channel
   * @throws  NonWritableChannelException
   * If this channel was not opened for writing
   * @throws  ClosedChannelException
   * If this channel is closed
   * @throws  IllegalArgumentException
   * If the new size is negative
   * @throws  IOException
   * If some other I/O error occurs
   */
  @throws[IOException]
  def truncate(size: Long): SeekableByteChannel = {
    throw new IOException("CephReadChannel: cannot change a read-only channel")
  }

  /**
   * Closes this channel.
   *
   * <p> After a channel is closed, any further attempt to invoke I/O
   * operations upon it will cause a {@link ClosedChannelException} to be
   * thrown.
   *
   * <p> If this channel is already closed then invoking this method has no
   * effect.
   *
   * <p> This method may be invoked at any time.  If some other thread has
   * already invoked it, however, then another invocation will block until
   * the first invocation is complete, after which it will return without
   * effect. </p>
   *
   * @throws  IOException If an I/O error occurs
   */
  @throws[IOException]
  override def close(): Unit = {
    channelIsOpen = false
    closeChannel()
  }

  /**
   * Ensure to close channel
   */
  private def closeChannel(): Unit = {
    if (channel != null) {
      try {
        channel.close()
      } finally {
        channel = null
      }
    }
  }
}
