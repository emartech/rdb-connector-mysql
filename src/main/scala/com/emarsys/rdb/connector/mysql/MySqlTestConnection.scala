package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.ConnectorResponse

import scala.concurrent.Future

trait MySqlTestConnection {
  self: MySqlConnector =>

  override def testConnection(): ConnectorResponse[Unit] = {
    Future.successful(Right())
  }
}