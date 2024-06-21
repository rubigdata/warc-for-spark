package org.rubigdata.warc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters.setAsJavaSetConverter

class WarcTable(path: String, parseHTTP: Boolean) extends Table with SupportsRead {

  lazy val sparkSession: SparkSession = SparkSession.active

  override def name(): String = this.getClass.toString

  override def schema(): StructType = getSchema

  override def columns(): Array[Column] = columnsFromSchema(getSchema)

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new WarcScanBuilder(sparkSession, path, getSchema)

  private def columnsFromSchema(schema: StructType): Array[Column] = {
    schema.fields.map(f => Column.create(f.name, f.dataType, f.nullable))
  }

  def getSchema: StructType = {
    val defaultFields = Seq(
      StructField("warc_id", StringType, nullable = false),
      StructField("warc_type", StringType, nullable = false),
      StructField("warc_target_uri", StringType, nullable = true),
      StructField("warc_date", TimestampType, nullable = false),
      StructField("content_type", StringType, nullable = false),
      StructField("warc_headers", MapType(StringType, ArrayType(StringType, containsNull = false), valueContainsNull = false), nullable = false),
    )

    val additionalFields = if (parseHTTP) {
      Seq(
        StructField("http_headers", MapType(StringType, ArrayType(StringType, containsNull = false), valueContainsNull = false), nullable = false),
        StructField("http_body", StringType, nullable = false)
      )
    } else {
      Seq(StructField("warc_body", StringType, nullable = false))
    }

    StructType(defaultFields ++ additionalFields)
  }

}
