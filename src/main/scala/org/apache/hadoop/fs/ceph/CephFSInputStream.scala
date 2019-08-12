package org.apache.hadoop.fs.ceph

import java.io.{EOFException, IOException}
import java.nio.ByteBuffer

import com.ceph.rados.IoCTX
import org.apache.hadoop.fs.FSInputStream

class CephFSInputStream(ioCtx: IoCTX, objectName: String, bufferSize: Int) extends FSInputStream {

  val channel = new CephReadChannel(ioCtx, objectName, bufferSize)

  // used for read() to read 1 byte
  val oneByteBuffer = new Array[Byte](1)

  /**
   * Seeks a different copy of the data.  Returns true if
   * found a new source, false otherwise.
   *
   * @param targetPos target new position
   */
  @throws[IOException]
  override def seekToNewSource(targetPos: Long) = false

  @throws[IOException]
  override def read(position: Long, buffer: Array[Byte], offset: Int, length: Int): Int = synchronized {
    validatePositionedReadArgs(position, buffer, offset, length)
    if (length == 0) return 0
    synchronized {
      val oldPos = getPos
      var numRead = -1
      try {
        seek(position)
        numRead = read(buffer, offset, length)
      } catch {
        case e: EOFException =>
        // end of file; this can be raised by some filesystems
        // (often: object stores); it is swallowed here.
        //        LOG.debug("Downgrading EOFException raised trying to" + " read {} bytes at offset {}", length, offset, e)
      } finally seek(oldPos)
      numRead
    }
  }

  /**
   * Seek to the given offset from the start of the file.
   * The next read() will be from that location.  Can't
   * seek past the end of the file.
   *
   * @param pos position to seek
   */
  @throws[IOException]
  override def seek(pos: Long): Unit = {
    try {
      channel.position(pos)
    } catch {
      case e: IllegalArgumentException => throw new IOException(e)
    }
  }

  /**
   * Return the current offset from the start of the file
   */
  @throws[IOException]
  override def getPos: Long = channel.position()

  /**
   * Reads the next byte of data from the input stream. The value byte is
   * returned as an <code>int</code> in the range <code>0</code> to
   * <code>255</code>. If no byte is available because the end of the stream
   * has been reached, the value <code>-1</code> is returned. This method
   * blocks until input data is available, the end of the stream is detected,
   * or an exception is thrown.
   *
   * <p> A subclass must provide an implementation of this method.
   *
   * @return the next byte of data, or <code>-1</code> if the end of the
   *         stream is reached.
   * @throws IOException if an I/O error occurs.
   */
  @throws[IOException]
  override def read: Int = {
    val numRead = channel.read(ByteBuffer.wrap(oneByteBuffer))
    numRead
  }
}
