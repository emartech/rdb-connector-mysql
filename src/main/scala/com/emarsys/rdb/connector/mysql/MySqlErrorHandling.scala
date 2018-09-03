package com.emarsys.rdb.connector.mysql

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.models.Errors._
import com.mysql.jdbc.exceptions.jdbc4.{MySQLSyntaxErrorException, MySQLTimeoutException}
import java.sql.SQLTransientConnectionException

trait MySqlErrorHandling {
  self: MySqlConnector =>

  protected def handleNotExistingTable[T](
      table: String
  ): PartialFunction[Throwable, Either[ConnectorError, T]] = {
    case e: Exception if e.getMessage.contains("doesn't exist") =>
      Left(TableNotFound(table))
  }

  private def errorHandler: PartialFunction[Throwable, ConnectorError] = {
    case ex: MySQLSyntaxErrorException                                              => SqlSyntaxError(ex.getMessage)
    case ex: MySQLTimeoutException                                                  => ConnectionTimeout(ex.getMessage)
    case ex: SQLTransientConnectionException if ex.getMessage.contains("timed out") => ConnectionTimeout(ex.getMessage)
    case ex                                                                         => ConnectionError(ex)
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[ConnectorError, T]] =
    errorHandler andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler andThen Source.failed

}
