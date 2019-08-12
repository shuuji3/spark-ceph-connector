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

  // TODO: implement
  /**
   * Reads up to <code>len</code> bytes of data from the input stream into
   * an array of bytes.  An attempt is made to read as many as
   * <code>len</code> bytes, but a smaller number may be read.
   * The number of bytes actually read is returned as an integer.
   *
   * <p> This method blocks until input data is available, end of file is
   * detected, or an exception is thrown.
   *
   * <p> If <code>len</code> is zero, then no bytes are read and
   * <code>0</code> is returned; otherwise, there is an attempt to read at
   * least one byte. If no byte is available because the stream is at end of
   * file, the value <code>-1</code> is returned; otherwise, at least one
   * byte is read and stored into <code>b</code>.
   *
   * <p> The first byte read is stored into element <code>b[off]</code>, the
   * next one into <code>b[off+1]</code>, and so on. The number of bytes read
   * is, at most, equal to <code>len</code>. Let <i>k</i> be the number of
   * bytes actually read; these bytes will be stored in elements
   * <code>b[off]</code> through <code>b[off+</code><i>k</i><code>-1]</code>,
   * leaving elements <code>b[off+</code><i>k</i><code>]</code> through
   * <code>b[off+len-1]</code> unaffected.
   *
   * <p> In every case, elements <code>b[0]</code> through
   * <code>b[off]</code> and elements <code>b[off+len]</code> through
   * <code>b[b.length-1]</code> are unaffected.
   *
   * <p> The <code>read(b,</code> <code>off,</code> <code>len)</code> method
   * for class <code>InputStream</code> simply calls the method
   * <code>read()</code> repeatedly. If the first such call results in an
   * <code>IOException</code>, that exception is returned from the call to
   * the <code>read(b,</code> <code>off,</code> <code>len)</code> method.  If
   * any subsequent call to <code>read()</code> results in a
   * <code>IOException</code>, the exception is caught and treated as if it
   * were end of file; the bytes read up to that point are stored into
   * <code>b</code> and the number of bytes read before the exception
   * occurred is returned. The default implementation of this method blocks
   * until the requested amount of input data <code>len</code> has been read,
   * end of file is detected, or an exception is thrown. Subclasses are encouraged
   * to provide a more efficient implementation of this method.
   *
   * @param      b   the buffer into which the data is read.
   * @param      off the start offset in array <code>b</code>
   *                 at which the data is written.
   * @param      len the maximum number of bytes to read.
   * @return the total number of bytes read into the buffer, or
   *         <code>-1</code> if there is no more data because the end of
   *         the stream has been reached.
   * @throws IOException               If the first byte cannot be read for any reason
   *                                   other than end of file, or if the input stream has been closed, or if
   *                                   some other I/O error occurs.
   * @throws NullPointerException      If <code>b</code> is <code>null</code>.
   * @throws IndexOutOfBoundsException If <code>off</code> is negative,
   *                                   <code>len</code> is negative, or <code>len</code> is greater than
   *                                   <code>b.length - off</code>
   * @see java.io.InputStream#read()
   */
  @throws[IOException]
  def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (b == null) throw new NullPointerException
    else if (off < 0 || len < 0 || len > b.length - off) throw new IndexOutOfBoundsException
    else if (len == 0) return 0
    var c = read
    if (c == -1) return -1
    b(off) = c.toByte
    var i = 1
    try
      while ( {
        i < len
      }) {
        c = read
        if (c == -1) {
          break
        } //todo: break is not supported
        b(off + i) = c.toByte

        {
          i += 1;
          i - 1
        }
      }
    catch {
      case ee: IOException =>

    }
    i
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
