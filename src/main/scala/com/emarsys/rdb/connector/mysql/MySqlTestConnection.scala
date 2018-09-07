package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.ConnectorResponse
import slick.jdbc.MySQLProfile.api._

trait MySqlTestConnection {
  self: MySqlConnector =>

  override def testConnection(): ConnectorResponse[Unit] = {
    db.run(sql"SELECT 1".as[Int])
      .map(_ => Right(()))
      .recover(eitherErrorHandler())
  }
}
