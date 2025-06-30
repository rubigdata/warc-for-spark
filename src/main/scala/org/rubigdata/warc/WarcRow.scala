package org.rubigdata.warc

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.jwat.common.HeaderLine
import org.jwat.warc.{WarcRecord => JwatRecord}
import org.netpreserve.jwarc.{MessageHeaders, WarcResponse, WarcTargetRecord, WarcRecord => JwarcRecord}

import java.sql.Timestamp
import java.time.{Instant, ZoneOffset}
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import scala.util.Try

abstract class WarcRow(options: WarcOptions) {
  import WarcRow._

  def warcId: String
  def warcType: String
  def warcTargetUri: String
  def warcDate: Instant
  def warcContentType: String
  def warcHeaders: Map[String, List[String]]
  def warcBody: Array[Byte]
  def httpContentType: String
  def httpHeaders: Map[String, List[String]]
  def httpBody: Array[Byte]

  def getDate: Timestamp = Timestamp.from(warcDate)

  def toInternalRow(schema: StructType): InternalRow = {
    InternalRow(schema.fieldNames.map(f => internalRepresentation(readField(f))): _*)
  }

  def readField(field: String): Any = field match {
    case WARC_ID => warcId
    case WARC_TYPE => warcType
    case WARC_TARGET_URI => warcTargetUri
    case WARC_DATE => warcDate
    case WARC_CONTENT_TYPE => warcContentType
    case WARC_HEADERS => warcHeaders
    case WARC_BODY => warcBody
    case HTTP_CONTENT_TYPE => httpContentType
    case HTTP_HEADERS => httpHeaders
    case HTTP_BODY => httpBody
    case _ => null
  }

  def readString(field: String): String = {
    readField(field) match {
      case s: String => s
      case null => null
      case _ => throw new IllegalArgumentException(s"Field $field is not a string field")
    }
  }

  private def internalRepresentation(value: Any): Any = value match {
    case s: String => UTF8String.fromString(s)
    case b: Array[Byte] => UTF8String.fromBytes(b)
    case m: Map[_, _] => encodeRecordHeaders(m.asInstanceOf[Map[String, List[String]]])
    case i: Instant => i.toEpochMilli * 1000L
    case _ => value
  }

  private def encodeRecordHeaders(headers: Map[String, List[String]]): ArrayBasedMapData = {
    val keys = new ListBuffer[UTF8String]()
    val values = new ListBuffer[ArrayData]()

    headers.foreach{ case (key, value) =>
      keys += UTF8String.fromString(if (options.headersToLowerCase) key.toLowerCase else key)
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
  val WARC_CONTENT_TYPE = "warcContentType"
  val WARC_HEADERS = "warcHeaders"
  val WARC_BODY = "warcBody"
  val HTTP_CONTENT_TYPE = "httpContentType"
  val HTTP_HEADERS = "httpHeaders"
  val HTTP_BODY = "httpBody"

  val STRING_FIELDS: Seq[String] = Seq(WARC_ID, WARC_TYPE, WARC_TARGET_URI, WARC_CONTENT_TYPE, HTTP_CONTENT_TYPE)
}

case class JwarcRow(warcRecord: JwarcRecord, options: WarcOptions) extends WarcRow(options) {

  lazy val warcId: String = warcRecord.id().toString
  lazy val warcType: String = warcRecord.`type`()
  lazy val warcTargetUri: String = warcRecord match {
    case r: WarcTargetRecord => r.target()
    case _ => null
  }
  lazy val warcDate: Instant = warcRecord.date()
  lazy val warcContentType: String = Try(warcRecord.contentType().toString).getOrElse(null)
  lazy val warcHeaders: Map[String, List[String]] = readHeaders(warcRecord.headers())
  lazy val warcBody: Array[Byte] = IOUtils.toByteArray(warcRecord.body().stream())
  lazy val httpContentType: String = warcRecord match {
    case r: WarcResponse => Try(r.http().contentType().toString).getOrElse(null)
    case _ => null
  }
  lazy val httpHeaders: Map[String, List[String]] = warcRecord match {
    case r: WarcResponse => readHeaders(r.http().headers())
    case _ => null
  }
  lazy val httpBody: Array[Byte] = warcRecord match {
    case r: WarcResponse => IOUtils.toByteArray(r.http().body().stream())
    case _ => null
  }

  private def readHeaders(headers: MessageHeaders): Map[String, List[String]] = {
    headers.map().asScala.toMap.mapValues(_.asScala.toList)
  }
}

case class JwatRow(warcRecord: JwatRecord, options: WarcOptions) extends WarcRow(options) {

  lazy val warcId: String = warcRecord.header.warcRecordIdStr.stripPrefix("<").stripSuffix(">")
  lazy val warcType: String = warcRecord.header.warcTypeStr
  lazy val warcTargetUri: String = warcRecord.header.warcTargetUriStr
  lazy val warcDate: Instant = Option(warcRecord.header.warcDate).map(_.ldt.toInstant(ZoneOffset.UTC)).orNull
  lazy val warcContentType: String = warcRecord.header.contentType.toString
  lazy val warcHeaders: Map[String, List[String]] = readHeaders(warcRecord.header.getHeaderList.asScala.toList)
  lazy val warcBody: Array[Byte] = IOUtils.toByteArray(warcRecord.getPayload.getInputStreamComplete)
  lazy val httpContentType: String = Option(warcRecord.getHttpHeader).map(_.contentType).orNull
  lazy val httpHeaders: Map[String, List[String]] = Option(warcRecord.getHttpHeader).map(h => readHeaders(h.getHeaderList.asScala.toList)).orNull
  lazy val httpBody: Array[Byte] = Option(warcRecord.getHttpHeader).map(_ => IOUtils.toByteArray(warcRecord.getPayloadContent)).orNull

  private def readHeaders(headers: List[HeaderLine]): Map[String, List[String]] = {
    headers.groupBy(_.name).mapValues(_.map(_.value))
  }
}

