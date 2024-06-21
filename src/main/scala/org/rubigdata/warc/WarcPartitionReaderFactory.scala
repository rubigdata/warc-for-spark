package org.rubigdata.warc

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class WarcPartitionReaderFactory(schema: StructType, filters: Array[Filter]) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition) : PartitionReader[InternalRow] =
    new WarcPartitionReader(partition.asInstanceOf[WarcPartition], schema, filters)
}
