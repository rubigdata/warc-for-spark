package org.rubigdata.warc

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import org.apache.spark.warc.WarcHelper

class WarcPartitionReaderFactory(options: WarcOptions, schema: StructType, filters: Array[Filter]) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition) : PartitionReader[InternalRow] = {
    val warcPartition = partition.asInstanceOf[WarcPartition]

    // Ensures that input_file_name() can be used
    WarcHelper.setCurrentFile(warcPartition.fileStatus)

    new WarcPartitionReader(warcPartition, options, schema, filters)
  }
}
