package org.rubigdata.warc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{GlobPattern, Path, RemoteIterator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, SupportsReportPartitioning}
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.connector.read.partitioning.{Partitioning, KeyGroupedPartitioning, UnknownPartitioning}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import scala.language.implicitConversions

class WarcScan(sparkSession: SparkSession, options: WarcOptions, schema: StructType, partitionFilters: Array[Filter], rowFilters: Array[Filter]) extends Scan
  with Batch with SupportsReportPartitioning {

  override def description(): String = {
    val buf = new StringBuilder(s"warc [${options.path}")

    if (options.lenient)
      buf ++= ", lenient"
    if (options.parseHTTP)
      buf ++= ", parseHTTP"
    if (options.filename)
      buf ++= ", filename"
    if (options.headersToLowerCase)
      buf ++= ", headersToLowerCase"
    options.pathGlobFilter.foreach{ glob =>
      buf ++= s", pathGlobFilter = $glob"
    }

    val partitionFiltersString = partitionFilters.mkString("[", ", ", "]")
    val pushedFiltersString = rowFilters.mkString("[", ", ", "]")
    val partitioningString = if (options.filename) s"[filename, ${partitions.length}]" else s"[${partitions.length}]"
    buf ++= s"], PartitionFilters: ${partitionFiltersString}, PushedFilters: ${pushedFiltersString}, Partitioning: ${partitioningString}, ReadSchema: ${schema.simpleString}"

    buf.toString()
  }

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  private val conf = sparkSession.sparkContext.hadoopConfiguration

  private lazy val files = {
    val path = new Path(options.path)
    val fs = path.getFileSystem(conf)

    options.pathGlobFilter match {
      case None => fs.listFiles(path, true).toSeq
      case Some(pattern) =>
        val globPattern = new GlobPattern(pattern)
        fs.listFiles(path, true).filter(status => globPattern.matches(status.getPath.toString)).toSeq
    }
  }

  private lazy val partitions: Array[InputPartition] = {
    val bcConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(conf))
    val filters = partitionFilters.flatMap(WarcPartitionReader.createPartitionFilterFunction)

    files.map { fileStatus =>
      WarcPartition(bcConf, fileStatus)
    }.filter { partition =>
      filters.forall(_.apply(partition))
    }.toArray
  }

  override def planInputPartitions(): Array[InputPartition] = partitions

  override def outputPartitioning(): Partitioning = {
    if (options.filename) {
      new KeyGroupedPartitioning(Array(Expressions.column("filename")), files.length)
    } else {
      new UnknownPartitioning(files.length)
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = new WarcPartitionReaderFactory(options, schema, rowFilters)

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
