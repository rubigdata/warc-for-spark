package org.rubigdata.warc

case class WarcOptions(
  path: String,
  parseHTTP: Boolean = false,
  lenient: Boolean = false,
  headersToLowerCase: Boolean = false,
  pathGlobFilter: Option[String] = None,
)

object WarcOptions {
  def parse(properties: Map[String, String]): WarcOptions = {
    val path: String = properties.get("path") match {
      case Some(p) => p
      case None => throw new IllegalArgumentException("Property 'path' is required")
    }

    val parseHTTP = parseBoolean(properties, "parseHTTP")
    val lenient = parseBoolean(properties, "lenient")
    val headersToLowerCase = parseBoolean(properties, "headersToLowerCase")
    val pathGlobFilter = properties.get("pathGlobFilter")

    WarcOptions(path, parseHTTP, lenient, headersToLowerCase, pathGlobFilter)
  }

  private def parseBoolean(properties: Map[String, String], propertyName: String, default: Boolean = false): Boolean = {
    properties.get(propertyName).map(_.toLowerCase) match {
      case Some("true") => true
      case Some("false") => false
      case None => default
      case _ => throw new IllegalArgumentException(s"Property '$propertyName' must be a boolean")
    }
  }

}
