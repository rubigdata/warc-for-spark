package org.rubigdata.warc

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter

class DefaultSource extends TableProvider {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    getTable(null, Array.empty, options.asCaseSensitiveMap()).asInstanceOf[WarcTable].getSchema
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    new WarcTable(WarcOptions.parse(properties.asScala.toMap))
  }

}
