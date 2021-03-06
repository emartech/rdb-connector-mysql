package com.emarsys.rdb.connector.mysql

import java.sql.SQLTransientException
import java.util.UUID

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors._
import com.emarsys.rdb.connector.common.models._
import com.emarsys.rdb.connector.mysql.MySqlConnector.{MySqlConnectionConfig, MySqlConnectorConfig}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import slick.jdbc.MySQLProfile.api._
import slick.jdbc.MySQLProfile.backend
import slick.util.AsyncExecutor

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class MySqlConnector(
    protected val db: Database,
    protected val connectorConfig: MySqlConnectorConfig,
    protected val poolName: String
)(implicit val executionContext: ExecutionContext)
    extends Connector
    with MySqlErrorHandling
    with MySqlTestConnection
    with MySqlMetadata
    with MySqlSimpleSelect
    with MySqlRawSelect
    with MySqlIsOptimized
    with MySqlRawDataManipulation {

  override protected val fieldValueConverters = MysqlFieldValueConverters

  override val isErrorRetryable: PartialFunction[Throwable, Boolean] = {
    case _: SQLTransientException                                  => true
    case _: ConnectionTimeout                                      => true
    case ErrorWithMessage(message) if message.contains("Deadlock") => true
    case _                                                         => false
  }

  override def close(): Future[Unit] = {
    db.shutdown
  }

  override def innerMetrics(): String = {
    import java.lang.management.ManagementFactory

    import com.zaxxer.hikari.HikariPoolMXBean
    import javax.management.{JMX, ObjectName}
    Try {
      val mBeanServer    = ManagementFactory.getPlatformMBeanServer
      val poolObjectName = new ObjectName(s"com.zaxxer.hikari:type=Pool ($poolName)")
      val poolProxy      = JMX.newMXBeanProxy(mBeanServer, poolObjectName, classOf[HikariPoolMXBean])

      s"""{
         |"activeConnections": ${poolProxy.getActiveConnections},
         |"idleConnections": ${poolProxy.getIdleConnections},
         |"threadAwaitingConnections": ${poolProxy.getThreadsAwaitingConnection},
         |"totalConnections": ${poolProxy.getTotalConnections}
         |}""".stripMargin
    }.getOrElse(super.innerMetrics)
  }
}

object MySqlConnector extends MySqlConnectorTrait {

  final case class MySqlConnectionConfig(
      host: String,
      port: Int,
      dbName: String,
      dbUser: String,
      dbPassword: String,
      certificate: String,
      connectionParams: String,
      replicaConfig: Option[MySqlConnectionConfig] = None
  ) extends ConnectionConfig {
    override def replica[C <: MySqlConnectionConfig]: Option[C] = replicaConfig.map(_.asInstanceOf[C])

    override def toCommonFormat: CommonConnectionReadableData = {
      CommonConnectionReadableData("mysql", s"$host:$port", dbName, dbUser)
    }
  }

  case class MySqlConnectorConfig(queryTimeout: FiniteDuration, streamChunkSize: Int)

}

trait MySqlConnectorTrait extends ConnectorCompanion with MySqlErrorHandling {

  val defaultConfig = MySqlConnectorConfig(
    queryTimeout = 20.minutes,
    streamChunkSize = 5000
  )

  val useSSL: Boolean = Config.db.useSsl

  override def meta() = MetaData("`", "'", "\\")

  def apply(
      config: MySqlConnectionConfig,
      connectorConfig: MySqlConnectorConfig = defaultConfig,
      configPath: String = "mysqldb"
  )(executor: AsyncExecutor)(implicit executionContext: ExecutionContext): ConnectorResponse[MySqlConnector] = {
    val keystoreUrl = CertificateUtil.createKeystoreTempUrlFromCertificateString(config.certificate)
    val poolName    = UUID.randomUUID.toString

    if (useSSL && keystoreUrl.isEmpty) {
      Future.successful(Left(ConnectionConfigError("Wrong SSL cert format")))
    } else {
      configureDb(config, configPath, keystoreUrl, poolName).flatMap(checkSslConfig(connectorConfig, poolName))
    }
  }

  private[mysql] def configureDb(
      config: MySqlConnectionConfig,
      configPath: String,
      keystoreUrlO: Option[String],
      poolName: String
  ) = {
    val keystoreUrl = keystoreUrlO.get
    val url: String = createUrl(config)
    val customDbConf = ConfigFactory
      .load()
      .getConfig(configPath)
      .withValue("poolName", ConfigValueFactory.fromAnyRef(poolName))
      .withValue("registerMbeans", ConfigValueFactory.fromAnyRef(true))
      .withValue("properties.url", ConfigValueFactory.fromAnyRef(url))
      .withValue("properties.user", ConfigValueFactory.fromAnyRef(config.dbUser))
      .withValue("properties.password", ConfigValueFactory.fromAnyRef(config.dbPassword))
      .withValue("properties.driver", ConfigValueFactory.fromAnyRef("slick.jdbc.MySQLProfile"))
    val sslConfig = if (useSSL) {
      customDbConf
        .withValue("properties.properties.useSSL", ConfigValueFactory.fromAnyRef("true"))
        .withValue("properties.properties.verifyServerCertificate", ConfigValueFactory.fromAnyRef("false"))
        .withValue(
          "properties.properties.clientCertificateKeyStoreUrl",
          ConfigValueFactory.fromAnyRef(keystoreUrl)
        )
    } else customDbConf

    Future.successful(Database.forConfig("", sslConfig))
  }

  private[mysql] def checkSslConfig(connectorConfig: MySqlConnectorConfig, poolName: String)(
      db: backend.Database
  )(implicit ec: ExecutionContext) = {
    isSslConfiguredProperly(db) map [Either[ConnectorError, MySqlConnector]] {
      if (_) {
        Right(new MySqlConnector(db, connectorConfig, poolName))
      } else {
        Left(ConnectionConfigError("SSL Error"))
      }
    } recover eitherErrorHandler() map {
      case Left(e) =>
        db.shutdown
        Left(e)
      case r => r
    }
  }

  private[mysql] def isSslConfiguredProperly(
      db: Database
  )(implicit executionContext: ExecutionContext): Future[Boolean] = {
    if (useSSL) {
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
