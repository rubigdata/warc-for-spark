package org.rubigdata.warc

import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable

case class WarcPartition(conf: Broadcast[SerializableConfiguration], fileStatus: LocatedFileStatus) extends InputPartition {

  override def preferredLocations(): Array[String] = {
    // Compute how many bytes of this file each host has
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    fileStatus.getBlockLocations.foreach { location =>
      location.getHosts.filter(_ != "localhost").foreach { host =>
        hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + location.getLength
      }
    }

    // Takes the first 3 hosts with the most data to be retrieved
    hostToNumBytes.toSeq.sortBy(_._2).reverse.take(3).map(_._1).toArray
  }

}
