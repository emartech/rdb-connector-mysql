package com.emarsys.rdb.connector.mysql

import java.sql.{SQLException, SQLTransientConnectionException}
import java.util.concurrent.RejectedExecutionException

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.models.Errors._
import com.mysql.jdbc.exceptions.MySQLTimeoutException
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException

trait MySqlErrorHandling {

  protected def handleNotExistingTable[T](
      table: String
  ): PartialFunction[Throwable, Either[ConnectorError, T]] = {
    case e: Exception if e.getMessage.contains("doesn't exist") =>
      Left(TableNotFound(table))
  }

  private def errorHandler: PartialFunction[Throwable, ConnectorError] = {
    case ex: slick.SlickException =>
      if (ex.getMessage == "Update statements should not return a ResultSet") {
        SqlSyntaxError("Wrong update statement: non update query given")
      } else {
        ErrorWithMessage(ex.getMessage)
      }
    case ex: MySQLSyntaxErrorException if ex.getMessage.contains("Access denied")   => AccessDeniedError(ex.getMessage)
    case ex: MySQLSyntaxErrorException                                              => SqlSyntaxError(ex.getMessage)
    case ex: MySQLTimeoutException if ex.getMessage.contains("cancelled")           => QueryTimeout(ex.getMessage)
    case ex: MySQLTimeoutException                                                  => ConnectionTimeout(ex.getMessage)
    case ex: RejectedExecutionException                                             => TooManyQueries(ex.getMessage)
    case ex: SQLTransientConnectionException if ex.getMessage.contains("timed out") => ConnectionTimeout(ex.getMessage)
    case ex: SQLException                                                           => ErrorWithMessage(s"[${ex.getSQLState}] - ${ex.getMessage}")
    case ex                                                                         => ErrorWithMessage(ex.getMessage)
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[ConnectorError, T]] =
    errorHandler andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler andThen Source.failed

}
