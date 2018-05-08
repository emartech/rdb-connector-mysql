package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.models.Errors.{ConnectionError, ConnectorError, TableNotFound}

trait MySqlErrorHandling {
  self: MySqlConnector =>

  protected def handleNotExistingTable[T](table: String): PartialFunction[Throwable, Either[ConnectorError,T]] = {
    case e: Exception if e.getMessage.contains("doesn't exist") => Left(TableNotFound(table))
  }

  protected def errorHandler[T](): PartialFunction[Throwable, Either[ConnectorError,T]] = {
    case ex: Exception => Left(ConnectionError(ex))
  }

}
