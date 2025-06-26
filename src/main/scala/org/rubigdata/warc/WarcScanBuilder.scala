package org.rubigdata.warc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class WarcScanBuilder(
                           sparkSession: SparkSession,
                           fileIndex: PartitioningAwareFileIndex,
                           schema: StructType,
                           dataSchema: StructType,
                           options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {

  override def build(): WarcScan = {
    WarcScan(
      sparkSession,
      fileIndex,
      dataSchema,
      readDataSchema(),
      readPartitionSchema(),
      options,
      pushedDataFilters,
      partitionFilters,
      dataFilters)
  }

  override def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = {
    dataFilters.filter(WarcPartitionReader.createFilterFunction(_).isDefined)
  }
}
