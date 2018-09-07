package com.emarsys.rdb.connector.mysql

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.SimpleSelect.FieldName
import slick.jdbc.MySQLProfile.api._

import scala.annotation.tailrec

trait MySqlRawSelect extends MySqlStreamingQuery {
  self: MySqlConnector =>

  import MySqlWriters._
  import com.emarsys.rdb.connector.common.defaults.SqlWriter._

  override def rawSelect(rawSql: String, limit: Option[Int]): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val query = removeEndingSemicolons(rawSql)
    val limitedQuery = limit.fold(query) { l =>
      wrapInLimit(query, l)
    }
    streamingQuery(limitedQuery)
  }

  override def validateRawSelect(rawSql: String): ConnectorResponse[Unit] = {
    val modifiedSql = wrapInExplain(removeEndingSemicolons(rawSql))
    runQueryOnDb(modifiedSql)
      .map(_ => Right(()))
      .recover(eitherErrorHandler())
  }

  private def runQueryOnDb(modifiedSql: String) = {
    db.run(sql"#$modifiedSql".as[Any])
  }

  private def wrapInExplain(sqlWithoutSemicolon: String) = {
    "EXPLAIN " + sqlWithoutSemicolon
  }

  override def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val modifiedSql = wrapInExplain(removeEndingSemicolons(rawSql))
    streamingQuery(modifiedSql)
  }

  private def runProjectedSelectWith[R](
      rawSql: String,
      fields: Seq[String],
      limit: Option[Int],
      allowNullFieldValue: Boolean,
      queryRunner: String => R
  ) = {
    val fieldList    = concatenateProjection(fields)
    val projectedSql = wrapInProjection(rawSql, fieldList)
    val query =
      if (!allowNullFieldValue) wrapInCondition(projectedSql, fields)
      else projectedSql
    val limitedQuery = limit.fold(query)(l => s"$query LIMIT $l")

    queryRunner(limitedQuery)
  }

  override def projectedRawSelect(
      rawSql: String,
      fields: Seq[String],
      limit: Option[Int],
      allowNullFieldValue: Boolean
  ): ConnectorResponse[Source[Seq[String], NotUsed]] =
    runProjectedSelectWith(rawSql, fields, limit, allowNullFieldValue, streamingQuery)

  override def validateProjectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Unit] = {
    val wrapInExplainThenRunOnDb = wrapInExplain _ andThen runQueryOnDb
    runProjectedSelectWith(rawSql, fields, None, allowNullFieldValue = true, wrapInExplainThenRunOnDb)
      .map(_ => Right(()))
      .recover(eitherErrorHandler())
  }

  private def concatenateProjection(fields: Seq[String]) =
    fields.map("t." + FieldName(_).toSql).mkString(", ")

  private def wrapInLimit(query: String, l: Int) =
    s"SELECT * FROM ( $query ) AS query LIMIT $l"

  private def wrapInCondition(rawSql: String, fields: Seq[String]): String =
    removeEndingSemicolons(rawSql) + concatenateCondition(fields)

  private def concatenateCondition(fields: Seq[String]) =
    " WHERE " + fields.map("t." + FieldName(_).toSql + " IS NOT NULL ").mkString("AND ")

  private def wrapInProjection(rawSql: String, projection: String) =
    s"SELECT $projection FROM ( ${removeEndingSemicolons(rawSql)} ) t"

  @tailrec
  private def removeEndingSemicolons(query: String): String = {
    val qTrimmed = query.trim
    if (qTrimmed.last == ';') {
      removeEndingSemicolons(qTrimmed.dropRight(1))
    } else {
      qTrimmed
    }
  }

}
