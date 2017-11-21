package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.mysql.Sanitizer.quoteIdentifier
import slick.jdbc.MySQLProfile.api._

trait MySqlIsOptimized {
  self: MySqlConnector =>

  override def isOptimized(table: String, fields: Seq[String]): ConnectorResponse[Boolean] = {
    val fieldSet = fields.toSet
    db.run(sql"SHOW INDEX FROM #${quoteIdentifier(table)}"
      .as[(String, String, String, String, String, String, String, String, String, String, String, String, String)])
      .map(_.groupBy(_._3).mapValues(_.map(_._5)).values)
      .map(_.exists(indexGroup => indexGroup.toSet == fieldSet || Set(indexGroup.head) == fieldSet))
      .map(Right(_))
      .recoverWith(handleNotExistingTable(table))
  }
}
