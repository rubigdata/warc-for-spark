package org.rubigdata.warc

import org.apache.hadoop.fs.Seekable
import org.apache.hadoop.io.compress.GzipCodec

import java.io.{IOException, InputStream}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, NonWritableChannelException, SeekableByteChannel}
import java.nio.charset.StandardCharsets

class WarcPartitionChannel[T <: InputStream with Seekable](in: T, start: Long, end: Long) extends SeekableByteChannel {

  in.seek(start)

  private val channel = Channels.newChannel(in)
  private var open = true

  override def read(byteBuffer: ByteBuffer): Int = {
    if (in.getPos >= end) {
      -1
    } else {
      byteBuffer.limit(byteBuffer.capacity() min (end - in.getPos).toInt)
      channel.read(byteBuffer)
    }
  }

  override def write(byteBuffer: ByteBuffer): Int = throw new NonWritableChannelException

  override def position(): Long = in.getPos - start

  override def position(l: Long): SeekableByteChannel = {
    in.seek(l + start)
    this
  }

  override def size(): Long = end - start

  override def truncate(l: Long): SeekableByteChannel = throw new NonWritableChannelException

  override def isOpen: Boolean = open

  override def close(): Unit = {
    in.close()
    open = false
  }

}

object WarcPartitionChannel {
  private val BUFFER_SIZE = 8192
  private val GZIP_BUFFER_SIZE = 256
  private val GZIP_HEADER = Array(0x1f.toByte, 0x8b.toByte)
  private val WARC_HEADER = "WARC/".getBytes(StandardCharsets.UTF_8)

  def fromUncompressed[T <: InputStream with Seekable](in: T, start: Long, end: Long): WarcPartitionChannel[T] = {
    val actualStart = {
      findByteSequence(in, WARC_HEADER, start)
      in.getPos
    }
    val actualEnd = {
      findByteSequence(in, WARC_HEADER, end)
      in.getPos
    }

    new WarcPartitionChannel[T](in, actualStart, actualEnd)
  }

  def fromGzip[T <: InputStream with Seekable](codec: GzipCodec, in: T, start: Long, end: Long): WarcPartitionChannel[T] = {
    val actualStart = findGzip(codec, in, start)
    val actualEnd = findGzip(codec, in, end)

    new WarcPartitionChannel[T](in, actualStart, actualEnd)
  }

  private def findGzip[T <: InputStream with Seekable](codec: GzipCodec, in: T, start: Long): Long = {
    val buffer = new Array[Byte](GZIP_BUFFER_SIZE)

    var pos = start

    while (findByteSequence(in, GZIP_HEADER, pos)) {
      val possiblePos = in.getPos

      try {
        val gzipStream = codec.createInputStream(in)
        gzipStream.read(buffer)

        in.seek(possiblePos)
        return possiblePos
      } catch {
        // If we get an exception, it means this is not a valid start of a gzip stream
        case _: IOException =>
      }

      pos = possiblePos + 1
    }
    in.getPos
  }

  private def findByteSequence[T <: InputStream with Seekable](in: T, sequence: Array[Byte], start: Long): Boolean = {
    val channel = Channels.newChannel(in)
    val buffer = ByteBuffer.allocate(BUFFER_SIZE)

    var currentPos = start
    in.seek(currentPos)

    var bytesRead = channel.read(buffer)
    buffer.flip()

    while (bytesRead >= 0) {
      for (i <- 0 to buffer.remaining() - sequence.length) {
        if (sequence.zipWithIndex.forall { case (b, j) => buffer.get(i + j) == b }) {
          in.seek(currentPos + i)
          return true
        }
      }

      val bytesToProceed = (buffer.remaining() - sequence.length + 1) max 0
      buffer.position(bytesToProceed)
      buffer.compact()
      currentPos += bytesToProceed

      bytesRead = channel.read(buffer)
      buffer.flip()
    }
    false
  }

}
