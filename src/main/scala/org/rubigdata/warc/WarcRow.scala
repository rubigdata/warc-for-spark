package org.rubigdata.warc

import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.netpreserve.jwarc.{MessageBody, MessageHeaders, WarcRecord, WarcResponse, WarcTargetRecord}

import java.io.DataInputStream
import java.sql.Timestamp
import java.time.Instant
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

case class WarcRow(warcRecord: WarcRecord) {

  import org.rubigdata.warc.WarcRow._

  private lazy val warcId = warcRecord.id().toString
  private lazy val warcType = warcRecord.`type`()
  private lazy val warcTargetUri = warcRecord match {
    case r: WarcTargetRecord => r.target()
    case _ => null
  }
  private lazy val warcDate = warcRecord.date()
  private lazy val contentType = warcRecord.contentType().toString
  private lazy val warcHeaders = readHeaders(warcRecord.headers())
  private lazy val warcBody = readBody(warcRecord.body())
  private lazy val httpHeaders = warcRecord match {
    case r: WarcResponse => readHeaders(r.http().headers())
    case _ => null
  }
  private lazy val httpBody = warcRecord match {
    case r: WarcResponse => readBody(r.http().body())
    case _ => null
  }

  def toInternalRow(schema: StructType): InternalRow = {
    InternalRow(schema.fieldNames.map(f => internalRepresentation(readField(f))): _*)
  }

  def readField(field: String): Any = field match {
    case WARC_ID => warcId
    case WARC_TYPE => warcType
    case WARC_TARGET_URI => warcTargetUri
    case WARC_DATE => warcDate
    case CONTENT_TYPE => contentType
    case WARC_HEADERS => warcHeaders
    case WARC_BODY => warcBody
    case HTTP_HEADERS => httpHeaders
    case HTTP_BODY => httpBody
    case _ => null
  }

  def readString(field: String): String = {
    readField(field) match {
      case s: String => s
      case _ => throw new IllegalArgumentException(s"Field $field is not a string field")
    }
  }

  def getDate: Timestamp = Timestamp.from(warcDate)

  private def internalRepresentation(value: Any): Any = value match {
    case s: String => UTF8String.fromString(s)
    case b: Array[Byte] => UTF8String.fromBytes(b)
    case m: Map[_, _] => parseRecordHeaders(m.asInstanceOf[Map[String, List[String]]])
    case i: Instant => i.toEpochMilli * 1000L
    case _ => value
  }

  private def readBody(body: MessageBody): Array[Byte] = {
    IOUtils.readFullyToByteArray(new DataInputStream(body.stream()))
  }

  private def readHeaders(headers: MessageHeaders): Map[String, List[String]] = {
    headers.map().asScala.toMap.mapValues(_.asScala.toList)
  }

  private def parseRecordHeaders(headers: Map[String, List[String]]): ArrayBasedMapData = {
    val keys = new ListBuffer[UTF8String]()
    val values = new ListBuffer[ArrayData]()

    headers.foreach{ case (key, value) =>
      keys += UTF8String.fromString(key)
      values += ArrayData.toArrayData(value.map(UTF8String.fromString).toArray)
    }

    new ArrayBasedMapData(ArrayData.toArrayData(keys.toArray), ArrayData.toArrayData(values.toArray))
  }
}

object WarcRow {
  val WARC_ID = "warcId"
  val WARC_TYPE = "warcType"
  val WARC_TARGET_URI = "warcTargetUri"
  val WARC_DATE = "warcDate"
  val CONTENT_TYPE = "contentType"
  val WARC_HEADERS = "warcHeaders"
  val WARC_BODY = "warcBody"
  val HTTP_HEADERS = "httpHeaders"
  val HTTP_BODY = "httpBody"

  val STRING_FIELDS: Seq[String] = Seq(WARC_ID, WARC_TYPE, WARC_TARGET_URI, CONTENT_TYPE)
}
