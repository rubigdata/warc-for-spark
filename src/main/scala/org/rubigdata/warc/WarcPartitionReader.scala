package org.rubigdata.warc

import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources.{And, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.netpreserve.jwarc.{MessageBody, MessageHeaders, WarcReader, WarcRecord, WarcResponse, WarcTargetRecord}

import java.io.DataInputStream
import java.sql.Timestamp
import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable.ListBuffer

class WarcPartitionReader(partition: WarcPartition, schema: StructType, filters: Array[Filter]) extends PartitionReader[InternalRow] {

  import WarcPartitionReader._

  private val reader: WarcReader = {
    val path = partition.fileStatus.getPath
    val fs = path.getFileSystem(partition.conf.value.value)

    new WarcReader(fs.open(path))
  }

  private var record: Option[WarcRecord] = None

  private val filterFuncs = filters.flatMap(createFilterFunction)

  private def readRecord(): Unit = {
    var record = reader.next()

    while (record.isPresent && !recordIsValid(record.get)) {
      record = reader.next()
    }

    this.record = if (record.isPresent) Some(record.get) else None
  }

  private def recordIsValid(record: WarcRecord): Boolean = {
    filterFuncs.forall(_.apply(record))
  }

  private def readField(record: WarcRecord, field: String): Any = field match {
    case _ if stringFields.contains(field) =>
      UTF8String.fromString(getStringValue(record, field))
    case "warc_date" => record.date().toEpochMilli * 1000L
    case "warc_headers" => parseRecordHeaders(record.headers())
    case "warc_body" => parseRecordBody(record.body())
    case "http_headers" => record match {
      case r: WarcResponse => parseRecordHeaders(r.http().headers())
      case _ => null
    }
    case "http_body" => record match {
      case r: WarcResponse => parseRecordBody(r.http().body())
      case _ => null
    }
  }

  def next: Boolean = {
    readRecord()
    record.isDefined
  }

  def get: InternalRow = {
    assert(record.isDefined)

    val r = record.get
    InternalRow(schema.fields.map(f => readField(r, f.name)): _*)
  }

  def close(): Unit = {
    reader.close()
  }

}

object WarcPartitionReader {
  private val stringFields = Seq("warc_id", "warc_type", "warc_target_uri", "content_type")

  def createFilterFunction(filter: Filter): Option[WarcRecord => Boolean] = {
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
      case IsNull("warc_target_uri") => Some(r => !r.isInstanceOf[WarcTargetRecord])
      case IsNull(_) => Some(_ => false)
      case IsNotNull("warc_target_uri") => Some(r => r.isInstanceOf[WarcTargetRecord])
      case IsNotNull(_) => Some(_ => true)
      case EqualTo(attribute, value: String) if stringFields.contains(attribute) =>
        Some(r => getStringValue(r, attribute).equals(value))
      case StringStartsWith(attribute, value: String) if stringFields.contains(attribute) =>
        Some(r => getStringValue(r, attribute).startsWith(value))
      case StringEndsWith(attribute, value: String) if stringFields.contains(attribute) =>
        Some(r => getStringValue(r, attribute).endsWith(value))
      case StringContains(attribute, value: String) if stringFields.contains(attribute) =>
        Some(r => getStringValue(r, attribute).contains(value))
      case LessThan("warc_date", value: Timestamp) =>
        Some(r => Timestamp.from(r.date()).getTime < value.getTime)
      case LessThanOrEqual("warc_date", value: Timestamp) =>
        Some(r => Timestamp.from(r.date()).getTime <= value.getTime)
      case GreaterThan("warc_date", value: Timestamp) =>
        Some(r => Timestamp.from(r.date()).getTime > value.getTime)
      case GreaterThanOrEqual("warc_date", value: Timestamp) =>
        Some(r => Timestamp.from(r.date()).getTime >= value.getTime)
      case EqualTo("warc_date", value: Timestamp) =>
        Some(r => Timestamp.from(r.date()).equals(value))
      case _ => None
    }
  }

  private def getStringValue(record: WarcRecord, field: String): String = field match {
    case "warc_id" => record.id().toString
    case "warc_type" => record.`type`()
    case "warc_target_uri" => record match {
      case r: WarcTargetRecord => r.target()
      case _ => null
    }
    case "content_type" => record.contentType().toString
  }

  private def parseRecordBody(body: MessageBody): UTF8String = {
    UTF8String.fromBytes(IOUtils.readFullyToByteArray(new DataInputStream(body.stream())))
  }

  private def parseRecordHeaders(headers: MessageHeaders): ArrayBasedMapData = {
    val headerMap = headers.map()

    val keys = new ListBuffer[UTF8String]()
    val values = new ListBuffer[ArrayData]()

    headerMap.entrySet().forEach(entry => {
      keys += UTF8String.fromString(entry.getKey)
      values += ArrayData.toArrayData(entry.getValue.map(UTF8String.fromString).toArray)
    })

    new ArrayBasedMapData(ArrayData.toArrayData(keys.toArray), ArrayData.toArrayData(values))
  }
}
