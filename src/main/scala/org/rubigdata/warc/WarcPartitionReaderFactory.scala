package org.rubigdata.warc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, PartitionReaderWithPartitionValues}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A factory used to create WARC readers.
 *
 * @param broadcastedConf Broadcasted serializable Hadoop Configuration.
 * @param readDataSchema Required data schema in the batch scan.
 * @param partitionSchema Schema of partitions.
 * @param options Options for parsing CSV files.
 */
case class WarcPartitionReaderFactory(broadcastedConf: Broadcast[SerializableConfiguration],
                                      readDataSchema: StructType,
                                      partitionSchema: StructType,
                                      options: WarcOptions,
                                      filters: Seq[Filter]) extends FilePartitionReaderFactory {

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val conf = broadcastedConf.value.value
    val fileReader = new WarcPartitionReader(conf, file, options, readDataSchema, filters)
    new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
      partitionSchema, file.partitionValues)
  }
}
