package org.rubigdata.warc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.{CompressionCodecFactory, GzipCodec}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.netpreserve.jwarc.{WarcReader => JwarcReader, WarcRecord => JwarcRecord}
import org.jwat.warc.{WarcReaderFactory, WarcReader => JwatReader, WarcRecord => JwatRecord}

import scala.collection.JavaConverters._
import java.io.Closeable
import java.nio.channels.Channels
import java.sql.Timestamp

abstract class WarcPartitionReader[R](schema: StructType, filters: Seq[Filter]) extends PartitionReader[InternalRow] {

  def createReader: java.lang.Iterable[R] with Closeable
  def toRow(record: R): WarcRow

  private val reader = createReader
  private val iterator = reader.iterator().asScala
  private var record: Option[WarcRow] = None

  private val filterFuncs = filters.flatMap(WarcPartitionReader.createFilterFunction)

  private def readRecord(): Unit = {
    record = if (iterator.hasNext) {
      Some(toRow(iterator.next()))
    } else {
      None
    }
  }

  private def recordIsValid(record: WarcRow): Boolean = {
    filterFuncs.forall(_.apply(record))
  }

  def next: Boolean = {
    readRecord()

    while (record.isDefined && !recordIsValid(record.get)) {
      readRecord()
    }

    record.isDefined
  }

  def get: InternalRow = {
    assert(record.isDefined)
    record.get.toInternalRow(schema)
  }

  def close(): Unit = {
    reader.close()
  }

}

object WarcPartitionReader {
  import WarcRow._

  def createFilterFunction(filter: Filter): Option[WarcRow => Boolean] = {
    filter match {
      case And(left, right) => (createFilterFunction(left), createFilterFunction(right)) match {
        case (Some(leftPred), Some(rightPred)) => Some(s => leftPred(s) && rightPred(s))
        case _ => None
      }
      case Or(left, right) => (createFilterFunction(left), createFilterFunction(right)) match {
        case (Some(leftPred), Some(rightPred)) => Some(s => leftPred(s) || rightPred(s))
        case _ => None
      }
      case Not(child) => createFilterFunction(child) match {
        case Some(pred) => Some(s => !pred(s))
        case _ => None
      }
      case IsNull(attribute) => Some(r => r.readField(attribute) == null)
      case IsNotNull(attribute) => Some(r => r.readField(attribute) != null)
      case EqualTo(attribute, value: String) if STRING_FIELDS.contains(attribute) =>
        Some(r => r.readString(attribute).equals(value))
      case StringStartsWith(attribute, value: String) if STRING_FIELDS.contains(attribute) =>
        Some(r => r.readString(attribute).startsWith(value))
      case StringEndsWith(attribute, value: String) if STRING_FIELDS.contains(attribute) =>
        Some(r => r.readString(attribute).endsWith(value))
      case StringContains(attribute, value: String) if STRING_FIELDS.contains(attribute) =>
        Some(r => r.readString(attribute).contains(value))
      case LessThan(WARC_DATE, value: Timestamp) =>
        Some(r => r.getDate.getTime < value.getTime)
      case LessThanOrEqual(WARC_DATE, value: Timestamp) =>
        Some(r => r.getDate.getTime <= value.getTime)
      case GreaterThan(WARC_DATE, value: Timestamp) =>
        Some(r => r.getDate.getTime > value.getTime)
      case GreaterThanOrEqual(WARC_DATE, value: Timestamp) =>
        Some(r => r.getDate.getTime >= value.getTime)
      case EqualTo(WARC_DATE, value: Timestamp) =>
        Some(r => r.getDate.equals(value))
      case _ => None
    }
  }
}

class JwarcPartitionReader(conf: Configuration, file: PartitionedFile, options: WarcOptions, schema: StructType, filters: Seq[Filter])
  extends WarcPartitionReader[JwarcRecord](schema, filters) {

  override def createReader: JwarcReader = {
    val channel = {
      val path = file.toPath
      val fs = path.getFileSystem(conf)
      val in = fs.open(path)

      new CompressionCodecFactory(conf).getCodec(path) match {
        case null =>
          WarcPartitionChannel.fromUncompressed(in, file.start, file.start + file.length)
        case codec: GzipCodec if options.splitGzip =>
          WarcPartitionChannel.fromGzip(codec, in, file.start, file.start + file.length)
        case codec =>
          Channels.newChannel(codec.createInputStream(in))
      }
    }

    val reader = new JwarcReader(channel)

    if (options.lenient) {
      reader.setLenient(true)
    }

    reader
  }

  override def toRow(record: JwarcRecord): WarcRow = JwarcRow(record, options)
}

class JwatPartitionReader(conf: Configuration, file: PartitionedFile, options: WarcOptions, schema: StructType, filters: Seq[Filter])
  extends WarcPartitionReader[JwatRecord](schema, filters) {

  override def createReader: JwatReader = {
    val in = {
      val path = file.toPath
      val fs = path.getFileSystem(conf)
      val in = fs.open(path)

      new CompressionCodecFactory(conf).getCodec(path) match {
        case null =>
          Channels.newInputStream(WarcPartitionChannel.fromUncompressed(in, file.start, file.start + file.length))
        case codec: GzipCodec if options.splitGzip =>
          Channels.newInputStream(WarcPartitionChannel.fromGzip(codec, in, file.start, file.start + file.length))
        case codec =>
          codec.createInputStream(in)
      }
    }

    WarcReaderFactory.getReader(in)
  }

  override def toRow(record: JwatRecord): WarcRow = JwatRow(record, options)
}
