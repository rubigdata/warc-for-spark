package org.rubigdata.warc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

class WarcScanBuilder(sparkSession: SparkSession, path: String, schema: StructType) extends ScanBuilder with SupportsPushDownFilters with SupportsPushDownRequiredColumns {

  private var targetSchema: StructType = schema
  private val selectedPushFilters = new ArrayBuffer[Filter]()

  override def build(): Scan = new WarcScan(sparkSession, path, targetSchema, pushedFilters())

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (possible, impossible) = filters.partition(f => WarcPartitionReader.createFilterFunction(f).isDefined)

    selectedPushFilters ++= possible

    impossible
  }

  override def pushedFilters(): Array[Filter] = selectedPushFilters.toArray

  override def pruneColumns(requiredSchema: StructType): Unit = {
    targetSchema = requiredSchema
  }

}
