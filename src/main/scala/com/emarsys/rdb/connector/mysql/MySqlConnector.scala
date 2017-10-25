package com.emarsys.rdb.connector.mysql

import java.util.Properties

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, ErrorWithMessage}
import slick.jdbc.MySQLProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.{ExecutionContext, Future}

class MySqlConnector(db: Database)(implicit executionContext: ExecutionContext) extends Connector {

  override def close(): Future[Unit] = {
    db.shutdown
  }

  override def testConnection(): ConnectorResponse[Unit] = {
    Future.successful(Right())
  }

}

object MySqlConnector {

  case class MySqlConnectionConfig(
                                    host: String,
                                    port: Int,
                                    dbName: String,
                                    user: String,
                                    password: String,
                                    certificate: String,
                                    connectionParams: String
                                  )

  def apply(config: MySqlConnectionConfig)(executor: AsyncExecutor)(implicit executionContext: ExecutionContext): ConnectorResponse[MySqlConnector] = {

    val prop = new Properties()
    prop.setProperty("useSSL", "true")
    prop.setProperty("serverSslCert", config.certificate)

    val url = createUrl(config)

    val db = Database.forURL(
      url = url,
      driver = "slick.jdbc.MySQLProfile",
      user = config.user,
      password = config.password,
      prop = prop,
      executor = executor
    )

    checkSsl(db).map[Either[ConnectorError, MySqlConnector]] {
      if (_) {
        Right(new MySqlConnector(db))
      } else {
        Left(ErrorWithMessage("SSL Error"))
      }
    }.recover {
      case _ => Left(ErrorWithMessage("Cannot connect to the sql server"))
    }
  }

  private[mysql] def checkSsl(db: Database)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    db.run(sql"SHOW STATUS LIKE 'ssl_cipher'".as[(String, String)])
      .map(ssl => ssl.head._2.contains("RSA-AES") || ssl.head._2.matches(".*AES\\d+-SHA.*"))
  }

  private[mysql] def createUrl(config: MySqlConnectionConfig) = {
    s"jdbc:mysql://${config.host}:${config.port}/${config.dbName}${safeConnectionParams(config.connectionParams)}"
  }

  private[mysql] def safeConnectionParams(connectionParams: String) = {
    if (connectionParams.startsWith("?") || connectionParams.isEmpty) {
      connectionParams
    } else {
      s"?$connectionParams"
    }
  }
}
