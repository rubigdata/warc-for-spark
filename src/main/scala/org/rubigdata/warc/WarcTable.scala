package org.rubigdata.warc

import scala.collection.JavaConverters._
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class WarcTable(
                     name: String,
                     sparkSession: SparkSession,
                     options: CaseInsensitiveStringMap,
                     paths: Seq[String],
                     userSpecifiedSchema: Option[StructType])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  import WarcRow._

  override def newScanBuilder(options: CaseInsensitiveStringMap): WarcScanBuilder =
    WarcScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    val parsedOptions = new WarcOptions(options.asScala.toMap)

    var fields = Seq(
      StructField(WARC_ID, StringType, nullable = false),
      StructField(WARC_TYPE, StringType, nullable = false),
      StructField(WARC_TARGET_URI, StringType, nullable = true),
      StructField(WARC_DATE, TimestampType, nullable = false),
      StructField(WARC_CONTENT_TYPE, StringType, nullable = false),
      StructField(WARC_HEADERS, MapType(StringType, ArrayType(StringType, containsNull = false), valueContainsNull = false), nullable = false),
    )

    if (parsedOptions.parseHTTP) {
      fields ++= Seq(
        StructField(HTTP_CONTENT_TYPE, StringType, nullable = true),
        StructField(HTTP_HEADERS, MapType(StringType, ArrayType(StringType, containsNull = false), valueContainsNull = false), nullable = true),
        StructField(HTTP_BODY, StringType, nullable = true)
      )
    } else {
      fields ++= Seq(StructField(WARC_BODY, StringType, nullable = false))
    }

    Some(StructType(fields))
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    throw new NotImplementedError("This file format does not support writes")

  override def formatName: String = "WARC"

  override def fallbackFileFormat: Class[_ <: FileFormat] =
    throw new NotImplementedError("This file format does not support a fallback V1 file format")

}
