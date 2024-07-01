package org.rubigdata.warc

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources.{And, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types.StructType
import org.netpreserve.jwarc.WarcReader

import java.sql.Timestamp

import WarcRow._

class WarcPartitionReader(partition: WarcPartition, options: WarcOptions, schema: StructType, filters: Array[Filter]) extends PartitionReader[InternalRow] {

  private val reader: WarcReader = {
    val path = partition.fileStatus.getPath
    val fs = path.getFileSystem(partition.conf.value.value)

    val reader = new WarcReader(fs.open(path))

    if (options.lenient) {
      reader.setLenient(true)
    }

    reader
  }

  private var record: Option[WarcRow] = None

  private val filterFuncs = filters.flatMap(WarcPartitionReader.createFilterFunction)

  private def readRecord(): Unit = {
    val nextRecord = reader.next()
    record = if (nextRecord.isPresent) Some(WarcRow(nextRecord.get)) else None
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
  def createFilterFunction(filter: Filter): Option[WarcRow => Boolean] = {
    filter match {
      case And(left, right) => (createFilterFunction(left), createFilterFunction(right)) match {
        case (Some(leftPred), Some(rightPred)) => Some(s => leftPred(s) && rightPred(s))
        case (Some(leftPred), None) => Some(leftPred)
        case (None, Some(rightPred)) => Some(rightPred)
        case (None, None) => Some(_ => true)
      }
      case Or(left, right) => (createFilterFunction(left), createFilterFunction(right)) match {
        case (Some(leftPred), Some(rightPred)) => Some(s => leftPred(s) || rightPred(s))
        case _ => Some(_ => true)
      }
      case Not(child) => createFilterFunction(child) match {
        case Some(pred) => Some(s => !pred(s))
        case _ => Some(_ => true)
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
