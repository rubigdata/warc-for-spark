package org.rubigdata.warc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.{Filter, And}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

class WarcScanBuilder(sparkSession: SparkSession, options: WarcOptions, schema: StructType) extends ScanBuilder with SupportsPushDownFilters with SupportsPushDownRequiredColumns {

  private var targetSchema: StructType = schema
  private val selectedPartitionFilters = new ArrayBuffer[Filter]()
  private val selectedRowFilters = new ArrayBuffer[Filter]()

  override def build(): Scan = new WarcScan(sparkSession, options, targetSchema, selectedPartitionFilters.toArray, selectedRowFilters.toArray)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val postScanFilters = new ArrayBuffer[Filter]()

    for (filter <- filters) {
      val (partitionFilters, rowFilters, postFilters) = pushFilter(filter)

      selectedPartitionFilters ++= partitionFilters
      selectedRowFilters ++= rowFilters
      postScanFilters ++= postFilters
    }

    postScanFilters.toArray
  }

  override def pushedFilters(): Array[Filter] = (selectedPartitionFilters ++ selectedRowFilters).toArray

  override def pruneColumns(requiredSchema: StructType): Unit = {
    targetSchema = requiredSchema
  }

  private def pushFilter(filter: Filter): (Seq[Filter], Seq[Filter], Seq[Filter]) = {
    if (WarcPartitionReader.createPartitionFilterFunction(filter).isDefined) {
      // Can be pushed down into partition pruning
      (Seq(filter), Seq.empty, Seq.empty)
    } else if (WarcPartitionReader.createRowFilterFunction(filter).isDefined) {
      // Can be pushed down into row pruning
      (Seq.empty, Seq(filter), Seq.empty)
    } else {
      filter match {
        // And filters can be combined in partition and row pruning
        case And(left, right) => {
          val (pLeft, rLeft, nLeft) = pushFilter(left)
          val (pRight, rRight, nRight) = pushFilter(right)

          (pLeft ++ pRight, rLeft ++ rRight, nLeft ++ nRight)
        }
        // If it's not a partition, row or conjunctive filter, we need to run it after the scan
        case _ => (Seq.empty, Seq.empty, Seq(filter))
      }
    }
  }
}
