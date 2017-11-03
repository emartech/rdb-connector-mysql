package com.emarsys.rdb.connector.mysql

object Sanitizer {

  def quoteIdentifier(identifier: String): String = {
    "`" + identifier.replace("""\""", "").replace("`", "``") + "`"
  }

  def quoteIdentifier(dbName: String, tableName: String): String = {
    quoteIdentifier(dbName) + "." + quoteIdentifier(tableName)
  }

}
