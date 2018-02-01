package com.emarsys.rdb.connector.mysql.utils

import java.util.Properties

import com.emarsys.rdb.connector.mysql.MySqlConnector.{MySqlConnectionConfig, createUrl}
import slick.jdbc.MySQLProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.Future

object TestHelper {
  import com.typesafe.config.ConfigFactory
  lazy val config = ConfigFactory.load()

  lazy val TEST_CONNECTION_CONFIG = MySqlConnectionConfig(
    host = config.getString("dbconf.host"),
    port= config.getString("dbconf.port").toInt,
    dbName= config.getString("dbconf.dbName"),
    dbUser= config.getString("dbconf.user"),
    dbPassword= config.getString("dbconf.password"),
    certificate= config.getString("dbconf.certificate"),
    connectionParams= config.getString("dbconf.connectionParams")
  )

  private lazy val executor = AsyncExecutor.default()
  private lazy val db: Database = {
    val prop = new Properties()
    prop.setProperty("useSSL", "true")
    prop.setProperty("serverSslCert", TEST_CONNECTION_CONFIG.certificate)
    //prop.setProperty("disableSslHostnameVerification", "true")

    val url = createUrl(TEST_CONNECTION_CONFIG)

    Database.forURL(
      url = url,
      driver = "slick.jdbc.MySQLProfile",
      user = TEST_CONNECTION_CONFIG.dbUser,
      password = TEST_CONNECTION_CONFIG.dbPassword,
      prop = prop,
      executor = executor
    )
  }

  def executeQuery(sql: String): Future[Int] = {
    db.run(sqlu"""#$sql""")
  }
}
