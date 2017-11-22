package com.emarsys.rdb.connector.mysql

import java.util.Properties

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, ErrorWithMessage, TableNotFound}
import com.emarsys.rdb.connector.common.models.{ConnectionConfig, Connector}
import com.emarsys.rdb.connector.mysql.MySqlConnector.MySqlConnectorConfig
import slick.jdbc.MySQLProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class MySqlConnector(
                      protected val db: Database,
                      protected val connectorConfig: MySqlConnectorConfig
                    )(
                      implicit val executionContext: ExecutionContext
                    )
  extends Connector
    with MySqlTestConnection
    with MySqlMetadata
    with MySqlSimpleSelect
    with MySqlRawSelect
    with MySqlIsOptimized {

  protected def handleNotExistingTable[T](table: String): PartialFunction[Throwable, ConnectorResponse[T]] = {
    case e: Exception if e.getMessage.contains("doesn't exist") =>
      Future.successful(Left(TableNotFound(table)))
  }

  override def close(): Future[Unit] = {
    db.shutdown
  }
}

object MySqlConnector {

  case class MySqlConnectionConfig(
                                    host: String,
                                    port: Int,
                                    dbName: String,
                                    dbUser: String,
                                    dbPassword: String,
                                    certificate: String,
                                    connectionParams: String
                                  ) extends ConnectionConfig

  case class MySqlConnectorConfig(
                                   queryTimeout: FiniteDuration,
                                   streamChunkSize: Int
                                 )

  private val defaultConfig = MySqlConnectorConfig(
    queryTimeout = 20.minutes,
    streamChunkSize = 5000
  )

  def apply(config: MySqlConnectionConfig, connectorConfig: MySqlConnectorConfig = defaultConfig)(executor: AsyncExecutor)(implicit executionContext: ExecutionContext): ConnectorResponse[MySqlConnector] = {

    val prop = new Properties()
    prop.setProperty("useSSL", "true")
    prop.setProperty("serverSslCert", config.certificate)

    val url = createUrl(config)

    val db = Database.forURL(
      url = url,
      driver = "slick.jdbc.MySQLProfile",
      user = config.dbUser,
      password = config.dbPassword,
      prop = prop,
      executor = executor
    )

    checkSsl(db).map[Either[ConnectorError, MySqlConnector]] {
      if (_) {
        Right(new MySqlConnector(db, connectorConfig))
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
