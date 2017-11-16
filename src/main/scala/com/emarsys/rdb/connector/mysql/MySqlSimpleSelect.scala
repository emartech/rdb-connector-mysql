package com.emarsys.rdb.connector.mysql

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.mysql.MySqlWriters._

trait MySqlSimpleSelect extends MySqlStreamingQuery {
  self: MySqlConnector =>

  override def simpleSelect(select: SimpleSelect): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    streamingQuery(select.toSql)
  }
}
