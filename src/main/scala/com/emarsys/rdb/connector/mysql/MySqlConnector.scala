package com.emarsys.rdb.connector.mysql

import java.util.{Properties, UUID}

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors._
import com.emarsys.rdb.connector.common.models._
import com.emarsys.rdb.connector.mysql.MySqlConnector.{MySqlConnectionConfig, MySqlConnectorConfig}
import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import slick.jdbc.MySQLProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class MySqlConnector(
                      protected val db: Database,
                      protected val connectorConfig: MySqlConnectorConfig,
                      protected val poolName: String
                    )(
                      implicit val executionContext: ExecutionContext
                    )
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
    case _: MySQLTransactionRollbackException => true
    case _ => false
  }

  override def close(): Future[Unit] = {
    db.shutdown
  }

  override def innerMetrics(): String = {
    import java.lang.management.ManagementFactory
    import com.zaxxer.hikari.HikariPoolMXBean
    import javax.management.{JMX, ObjectName}
    Try {
      val mBeanServer = ManagementFactory.getPlatformMBeanServer
      val poolObjectName = new ObjectName(s"com.zaxxer.hikari:type=Pool ($poolName)")
      val poolProxy = JMX.newMXBeanProxy(mBeanServer, poolObjectName, classOf[HikariPoolMXBean])

      s"""{
         |"activeConnections": ${poolProxy.getActiveConnections},
         |"idleConnections": ${poolProxy.getIdleConnections},
         |"threadAwaitingConnections": ${poolProxy.getThreadsAwaitingConnection},
         |"totalConnections": ${poolProxy.getTotalConnections}
         |}""".stripMargin
    }.getOrElse(super.innerMetrics)
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
                                  ) extends ConnectionConfig {
    override def toCommonFormat: CommonConnectionReadableData = {
      CommonConnectionReadableData("mysql", s"$host:$port", dbName, dbUser)
    }
  }

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

    val keystoreUrlO = CertificateUtil.createKeystoreTempUrlFromCertificateString(config.certificate)
    val poolName = UUID.randomUUID.toString

    if (useSSL && keystoreUrlO.isEmpty) {
      Future.successful(Left(ConnectionConfigError("Wrong SSL cert format")))
    } else {
      val keystoreUrl = keystoreUrlO.get
      val url: String = createUrl(config)
      val db =
        if(!useHikari) {
          val prop = new Properties()
          prop.setProperty("useSSL", "true")
          prop.setProperty("verifyServerCertificate", "false")
          prop.setProperty("clientCertificateKeyStoreUrl", keystoreUrl)

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
            .withValue("mysqldb.poolName", ConfigValueFactory.fromAnyRef(poolName))
            .withValue("mysqldb.registerMbeans", ConfigValueFactory.fromAnyRef(true))
            .withValue("mysqldb.properties.url", ConfigValueFactory.fromAnyRef(url))
            .withValue("mysqldb.properties.user", ConfigValueFactory.fromAnyRef(config.dbUser))
            .withValue("mysqldb.properties.password", ConfigValueFactory.fromAnyRef(config.dbPassword))
            .withValue("mysqldb.properties.driver", ConfigValueFactory.fromAnyRef("slick.jdbc.MySQLProfile"))
          val sslConfig = if (useSSL) {
            customDbConf
              .withValue("mysqldb.properties.properties.useSSL", ConfigValueFactory.fromAnyRef("true"))
              .withValue("mysqldb.properties.properties.verifyServerCertificate", ConfigValueFactory.fromAnyRef("false"))
              .withValue("mysqldb.properties.properties.clientCertificateKeyStoreUrl", ConfigValueFactory.fromAnyRef(keystoreUrl))
          } else customDbConf
          Database.forConfig("mysqldb", sslConfig)
        }

      checkSsl(db).map[Either[ConnectorError, MySqlConnector]] {
        if (_) {
          Right(new MySqlConnector(db, connectorConfig, poolName))
        } else {
          Left(ConnectionConfigError("SSL Error"))
        }
      }.recover {
        case ex => Left(ConnectionError(ex))
      }
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
