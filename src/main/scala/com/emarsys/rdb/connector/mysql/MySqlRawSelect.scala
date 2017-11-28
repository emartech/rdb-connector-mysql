package com.emarsys.rdb.connector.mysql

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.common.models.SimpleSelect.FieldName
import slick.jdbc.MySQLProfile.api._

import scala.annotation.tailrec

trait MySqlRawSelect extends MySqlStreamingQuery {
  self: MySqlConnector =>

  import MySqlWriters._
  import com.emarsys.rdb.connector.common.defaults.SqlWriter._


  override def rawSelect(rawSql: String, limit: Option[Int]): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val limitAsSql = limit.fold(""){ l =>
      s" LIMIT $l"
    }
    streamingQuery(removeEndingSemicolons(rawSql) + limitAsSql)
  }

  override def validateRawSelect(rawSql: String): ConnectorResponse[Unit] = {
    val modifiedSql = "EXPLAIN " + removeEndingSemicolons(rawSql)
    db.run(sql"#$modifiedSql".as[Any])
      .map(_ => Right())
      .recover {
        case ex => Left(ErrorWithMessage(ex.toString))
      }
  }

  override def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val modifiedSql = "EXPLAIN " + removeEndingSemicolons(rawSql)
    streamingQuery(modifiedSql)
  }

  override def projectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val fieldList = fields.map("t." + FieldName(_).toSql).mkString(", ")
    val projectedSql = s"SELECT $fieldList FROM ( ${removeEndingSemicolons(rawSql)} ) t"
    streamingQuery(projectedSql)
  }

  @tailrec
  private def removeEndingSemicolons(query: String): String = {
    val qTrimmed = query.trim
    if(qTrimmed.last == ';') {
      removeEndingSemicolons(qTrimmed.dropRight(1))
    } else {
      qTrimmed
    }
  }

}
