package org.rubigdata.warc

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DefaultSource extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] =
    throw new NotImplementedError("This file format does not support a fallback V1 file format")

  override def shortName(): String = "warc"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    WarcTable(tableName, sparkSession, optionsWithoutPaths, paths, None)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    WarcTable(tableName, sparkSession, optionsWithoutPaths, paths, Some(schema))
  }
}
