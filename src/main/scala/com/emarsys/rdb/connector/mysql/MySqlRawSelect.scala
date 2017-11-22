package com.emarsys.rdb.connector.mysql

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage

import slick.jdbc.MySQLProfile.api._

trait MySqlRawSelect extends MySqlStreamingQuery {
  self: MySqlConnector =>

  override def rawSelect(rawSql: String, limit: Option[Int]): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val limitAsSql = limit.fold(""){ l =>
      s" LIMIT $l"
    }
    val trimed = rawSql.trim
    streamingQuery(trimed.patch(trimed.lastIndexOf(';'), "", 1) + limitAsSql)
  }

  override def validateRawSelect(rawSql: String): ConnectorResponse[Unit] = {
    val trimed = rawSql.trim
    val modifiedSql = "EXPLAIN " + trimed.patch(trimed.lastIndexOf(';'), "", 1)
    db.run(sql"#$modifiedSql".as[Any])
      .map(_ => Right())
      .recover {
        case ex => Left(ErrorWithMessage(ex.toString))
      }
  }

}
