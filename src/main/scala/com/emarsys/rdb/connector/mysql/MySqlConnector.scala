package com.emarsys.rdb.connector.mysql

import java.util.Properties

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, ErrorWithMessage, TableNotFound}
import com.emarsys.rdb.connector.common.models.{ConnectionConfig, Connector, ConnectorCompanion, MetaData}
import com.emarsys.rdb.connector.mysql.MySqlConnector.{MySqlConnectionConfig, MySqlConnectorConfig}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
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
    with MySqlIsOptimized
    with MySqlRawDataManipulation {

  protected def handleNotExistingTable[T](table: String): PartialFunction[Throwable, ConnectorResponse[T]] = {
    case e: Exception if e.getMessage.contains("doesn't exist") =>
      Future.successful(Left(TableNotFound(table)))
  }

  override def close(): Future[Unit] = {
    db.shutdown
  }
}

object MySqlConnector extends MySqlConnectorTrait{
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

}

trait MySqlConnectorTrait extends ConnectorCompanion {

  val defaultConfig = MySqlConnectorConfig(
    queryTimeout = 20.minutes,
    streamChunkSize = 5000
  )

  val useSSL: Boolean = Config.db.useSsl
  val useHikari: Boolean = Config.db.useHikari

  def apply(config: MySqlConnectionConfig, connectorConfig: MySqlConnectorConfig = defaultConfig)(executor: AsyncExecutor)(implicit executionContext: ExecutionContext): ConnectorResponse[MySqlConnector] = {

    val prop = new Properties()
    prop.setProperty("useSSL", "true")
    prop.setProperty("serverSslCert", config.certificate)
    prop.setProperty("disableSslHostnameVerification", "true")

    val url = createUrl(config)

    val db =
      if(!useHikari) {
        Database.forURL(
          url = url,
          driver = "slick.jdbc.MySQLProfile",
          user = config.dbUser,
          password = config.dbPassword,
          prop = prop,
          executor = executor
        )
      } else {
        val customDbConf = ConfigFactory.load()
          .withValue("mysqldb.properties.url", ConfigValueFactory.fromAnyRef(url))
          .withValue("mysqldb.properties.user", ConfigValueFactory.fromAnyRef(config.dbUser))
          .withValue("mysqldb.properties.password", ConfigValueFactory.fromAnyRef(config.dbPassword))
          .withValue("mysqldb.properties.driver", ConfigValueFactory.fromAnyRef("slick.jdbc.MySQLProfile"))
        val sslConfig = if (Config.db.useSsl) {
          customDbConf
            .withValue("mysqldb.properties.properties.useSSL", ConfigValueFactory.fromAnyRef("true"))
            .withValue("mysqldb.properties.properties.disableSslHostnameVerification", ConfigValueFactory.fromAnyRef("true"))
            .withValue("mysqldb.properties.properties.serverSslCert", ConfigValueFactory.fromAnyRef(config.certificate))
        } else customDbConf
        Database.forConfig("mysqldb", sslConfig)
      }

    checkSsl(db).map[Either[ConnectorError, MySqlConnector]] {
      if (_) {
        Right(new MySqlConnector(db, connectorConfig))
      } else {
        Left(ErrorWithMessage("SSL Error"))
      }
    }.recover {
      case ex => Left(ErrorWithMessage(s"Cannot connect to the sql server: ${ex.getMessage}"))
    }
  }

  override def meta() = MetaData("`", "'", "\\")

  private[mysql] def checkSsl(db: Database)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    if(useSSL) {
      db.run(sql"SHOW STATUS LIKE 'ssl_cipher'".as[(String, String)])
        .map(ssl => ssl.head._2.contains("RSA-AES") || ssl.head._2.matches(".*AES\\d+-SHA.*"))
    } else {
      Future.successful(true)
    }
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
