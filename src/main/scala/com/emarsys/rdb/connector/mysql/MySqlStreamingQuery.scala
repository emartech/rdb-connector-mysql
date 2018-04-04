package com.emarsys.rdb.connector.mysql

import java.sql.Types

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import slick.jdbc.{GetResult, PositionedResult}
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Future

trait MySqlStreamingQuery {
  self: MySqlConnector =>

  protected def streamingQuery(query: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val sql = sql"#$query"
      .as(resultConverter)
      .transactionally
      .withStatementParameters(fetchSize = Int.MinValue)

    val publisher = db.stream(sql)
    val dbSource = Source
      .fromPublisher(publisher)
      .idleTimeout(connectorConfig.queryTimeout)
      .initialTimeout(connectorConfig.queryTimeout)
      .statefulMapConcat {
        () =>
          var first = true
          (data: (Seq[String], Seq[String])) =>
            if (first) {
              first = false
              List(data._1, data._2)
            } else {
              List(data._2)
            }
      }

    Future.successful(Right(dbSource))
  }

  private val resultConverter: GetResult[(Seq[String], Seq[String])] = GetResult[(Seq[String], Seq[String])] { r =>
    val rowData = getRowData(r)
    val headers = getHeaders(r)
    (headers, rowData)
  }

  private def getRowData(result: PositionedResult): Seq[String] = {
    val columnTypes = (1 to result.numColumns).map(result.rs.getMetaData.getColumnType(_))

    (0 until result.numColumns).map { i =>
      if (columnTypes(i) == Types.TIMESTAMP) {
        parseDateTime(result.nextString())
      } else {
        result.nextString()
      }
    }
  }

  private def getHeaders(r: PositionedResult): Seq[String] =
    (1 to r.numColumns).map(r.rs.getMetaData.getColumnLabel(_))

  private def parseDateTime(column: String): String = Option(column) match {
    case Some(s) => s.split('.').headOption.getOrElse("")
    case None => null
  }
}
