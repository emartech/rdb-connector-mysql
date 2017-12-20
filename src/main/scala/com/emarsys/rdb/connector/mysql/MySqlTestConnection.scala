package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import slick.jdbc.MySQLProfile.api._

trait MySqlTestConnection {
  self: MySqlConnector =>

  override def testConnection(): ConnectorResponse[Unit] = {
    db.run(sql"SELECT 1".as[Int]).map(_ => Right(()))
      .recover {
        case ex => Left(ErrorWithMessage(ex.toString))
      }
  }
}