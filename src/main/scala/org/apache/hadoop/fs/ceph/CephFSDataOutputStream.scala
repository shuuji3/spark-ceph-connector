package org.apache.hadoop.fs.ceph

import java.io.{IOException, OutputStream}
import java.nio.ByteBuffer

import com.ceph.rados.IoCTX
import org.apache.hadoop.fs.Seekable

class CephFSDataOutputStream(ioCtx: IoCTX, objectName: String, bufferSize: Int)
  extends OutputStream with Seekable {

  val channel = new CephWriteChannel(ioCtx, objectName, bufferSize)

  // used for read() to read 1 byte
  val oneByteBuffer = new Array[Byte](1)

  /**
   * Return the current offset from the start of the file
   */
  @throws[IOException]
  override def getPos: Long = channel.position()

  /**
   * Writes <code>len</code> bytes from the specified byte array
   * starting at offset <code>off</code> to this output stream.
   * The general contract for <code>write(b, off, len)</code> is that
   * some of the bytes in the array <code>b</code> are written to the
   * output stream in order; element <code>b[off]</code> is the first
   * byte written and <code>b[off+len-1]</code> is the last byte written
   * by this operation.
   * <p>
   * The <code>write</code> method of <code>OutputStream</code> calls
   * the write method of one argument on each of the bytes to be
   * written out. Subclasses are encouraged to override this method and
   * provide a more efficient implementation.
   * <p>
   * If <code>b</code> is <code>null</code>, a
   * <code>NullPointerException</code> is thrown.
   * <p>
   * If <code>off</code> is negative, or <code>len</code> is negative, or
   * <code>off+len</code> is greater than the length of the array
   * <code>b</code>, then an <tt>IndexOutOfBoundsException</tt> is thrown.
   *
   * @param      buf    the data.
   * @param      offset the start offset in the data.
   * @param      length the number of bytes to write.
   * @throws IOException if an I/O error occurs. In particular,
   *                     an <code>IOException</code> is thrown if the output
   *                     stream is closed.
   */
  @throws[IOException]
  override def write(buf: Array[Byte], offset: Int, length: Int): Unit = {
    if (buf == null) {
      throw new NullPointerException
    } else if (length == 0) {
      return
    }

    write(ByteBuffer.wrap(buf, offset, length))
  }

  /**
   * Writes the data of ByteBuffer into Rados object via channel
   *
   * @param src ByteBuffer of data to save into Rados object
   * @return
   */
  def write(src: ByteBuffer): Unit = {
    channel.write(src)
  }

  /**
   * Writes the specified byte to this output stream. The general
   * contract for <code>write</code> is that one byte is written
   * to the output stream. The byte to be written is the eight
   * low-order bits of the argument <code>b</code>. The 24
   * high-order bits of <code>b</code> are ignored.
   * <p>
   * Subclasses of <code>OutputStream</code> must provide an
   * implementation for this method.
   *
   * @param b the <code>byte</code>.
   * @throws IOException if an I/O error occurs. In particular,
   *                     an <code>IOException</code> may be thrown if the
   *                     output stream has been closed.
   */
  @throws[IOException]
  override def write(b: Int): Unit = {
    oneByteBuffer(0) = b.toByte
    write(ByteBuffer.wrap(oneByteBuffer))
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
    channel.position(pos)
  }

  /**
   * Seeks a different copy of the data.  Returns true if
   * found a new source, false otherwise.
   *
   * @param targetPos target new position
   */
  @throws[IOException]
  override def seekToNewSource(targetPos: Long) = false
}
