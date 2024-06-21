package org.rubigdata.warc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, RemoteIterator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import scala.language.implicitConversions

class WarcScan(sparkSession: SparkSession, source: String, schema: StructType, filters: Array[Filter]) extends Scan with Batch {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val conf: Configuration = sparkSession.sparkContext.hadoopConfiguration

    val path = new Path(source)
    val fs = path.getFileSystem(conf)

    fs.listFiles(path, true).map { fileStatus =>
      WarcPartition(sparkSession.sparkContext.broadcast(new SerializableConfiguration(conf)), fileStatus)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = new WarcPartitionReaderFactory(schema, filters)

  /**
   * Turns a [[RemoteIterator]] into a regular Scala [[Iterator]].
   *
   * @param ri the [[RemoteIterator]] to wrap
   * @tparam A the underlying type of the iterator
   * @return a Scala [[Iterator]], wrapping the input [[RemoteIterator]]
   */
  private implicit def remoteIteratorToIterator[A](ri: RemoteIterator[A]): Iterator[A] = new Iterator[A] {
    override def hasNext: Boolean = ri.hasNext

    override def next(): A = ri.next()
  }

}
