package org.rubigdata.warc

case class WarcOptions(path: String, parseHTTP: Boolean = false, pathGlobFilter: Option[String] = None)

object WarcOptions {
  def parse(properties: Map[String, String]): WarcOptions = {
    val path: String = properties.get("path").orNull

    val parseHTTP = properties.getOrElse("parseHTTP", "false").toLowerCase match {
      case "true" => true
      case "false" => false
      case _ => throw new IllegalArgumentException("parseHTTP must be a boolean")
    }

    val pathGlobFilter = properties.get("pathGlobFilter")

    WarcOptions(path, parseHTTP, pathGlobFilter)
  }
}
