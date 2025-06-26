package org.rubigdata.warc

import scala.collection.JavaConverters._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{CompressionCodecFactory, GzipCodec}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{ExprUtils, Expression}
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.TextBasedFileScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

case class WarcScan(
                    sparkSession: SparkSession,
                    fileIndex: PartitioningAwareFileIndex,
                    dataSchema: StructType,
                    readDataSchema: StructType,
                    readPartitionSchema: StructType,
                    options: CaseInsensitiveStringMap,
                    pushedFilters: Array[Filter],
                    partitionFilters: Seq[Expression] = Seq.empty,
                    dataFilters: Seq[Expression] = Seq.empty)
  extends TextBasedFileScan(sparkSession, options) {

  @transient private lazy val codecFactory: CompressionCodecFactory = new CompressionCodecFactory(
    sparkSession.sessionState.newHadoopConfWithOptions(options.asScala.toMap))

  override def isSplitable(path: Path): Boolean = {
    val codec = codecFactory.getCodec(path)
    val parsedOptions = new WarcOptions(options.asScala.toMap)

    codec == null || codec.isInstanceOf[GzipCodec] && parsedOptions.splitGzip
  }

  override def getFileUnSplittableReason(path: Path): String = {
    assert(!isSplitable(path))

    val codec = codecFactory.getCodec(path)

    if (codec.isInstanceOf[GzipCodec]) {
      "the splitGzip option is not enabled"
    } else {
      s"file splitting is not supported for ${codec.getClass.getSimpleName}"
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    val parsedOptions = new WarcOptions(options.asScala.toMap)

    WarcPartitionReaderFactory(broadcastedConf, readDataSchema, readPartitionSchema, parsedOptions, pushedFilters)
  }

  override def equals(obj: Any): Boolean = obj match {
    case c: WarcScan => super.equals(c) && dataSchema == c.dataSchema && options == c.options &&
      equivalentFilters(pushedFilters, c.pushedFilters)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def getMetaData(): Map[String, String] = {
    super.getMetaData() ++ Map("PushedFilters" -> seqToString(pushedFilters))
  }
}
