package org.apache.spark.warc

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.rdd.InputFileBlockHolder

/**
 * Helper class that allows access to package-private InputFileBlockHolder,
 * which is needed to be able to support input_file_name().
 */
object WarcHelper {
  def setCurrentFile(status: FileStatus): Unit = {
    InputFileBlockHolder.set(status.getPath.toString, 0, status.getLen)
  }
}
