package org.rubigdata.warc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{DataSourceOptions, FileSourceOptions}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import java.util.Locale

class WarcOptions(@transient val parameters: CaseInsensitiveMap[String])
  extends FileSourceOptions(parameters) with Logging {

  import WarcOptions._

  def this(parameters: Map[String, String]) = {
    this(CaseInsensitiveMap(parameters))
  }

  private def getBool(paramName: String, default: Boolean = false): Boolean = {
    val param = parameters.getOrElse(paramName, default.toString)
    if (param == null) {
      default
    } else if (param.toLowerCase(Locale.ROOT) == "true") {
      true
    } else if (param.toLowerCase(Locale.ROOT) == "false") {
      false
    } else {
      throw new IllegalArgumentException(s"Parameter '$paramName' must be a boolean")
    }
  }

  val headersToLowerCase: Boolean = getBool(HEADERS_TO_LOWER_CASE)
  val parseHTTP: Boolean = getBool(PARSE_HTTP)
  val lenient: Boolean = getBool(LENIENT)
  val splitGzip: Boolean = getBool(SPLIT_GZIP)

  val parser: String = parameters.getOrElse("parser", "jwarc")
}

object WarcOptions extends DataSourceOptions {
  private val HEADERS_TO_LOWER_CASE: String = newOption("headersToLowerCase")
  private val PARSE_HTTP: String = newOption("parseHTTP")
  private val LENIENT: String = newOption("lenient")
  private val RAW_CONTENT_TYPES: String = newOption("rawContentTypes")
  private val SPLIT_GZIP: String = newOption("splitGzip")
}
