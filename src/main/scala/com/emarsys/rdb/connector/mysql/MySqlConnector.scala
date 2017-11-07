package com.emarsys.rdb.connector.mysql

import java.util.Properties

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.{ConnectionConfig, Connector}
import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, ErrorWithMessage}
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel, TableModel}
import com.emarsys.rdb.connector.mysql.Sanitizer._
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

  override def listTables(): ConnectorResponse[Seq[TableModel]] = {
    db.run(sql"SHOW FULL TABLES".as[(String, String)])
      .map(_.map(parseToTableModel))
      .map(Right(_))
      .recover {
        case ex => Left(ErrorWithMessage(ex.toString))
      }
  }

  override def listFields(tableName: String): ConnectorResponse[Seq[FieldModel]] = {
    db.run(sql"DESC #${quoteIdentifier(tableName)}".as[(String, String, String, String, String, String)])
      .map(_.map(parseToFiledModel))
      .map(Right(_))
      .recover {
        case ex => Left(ErrorWithMessage(ex.toString))
      }
  }

  override def listTablesWithFields(): ConnectorResponse[Seq[FullTableModel]] = {
    val futureMap = listAllFields()
    for {
      tablesE <- listTables()
      map <- futureMap
    } yield tablesE.map(makeTablesWithFields(_, map))
  }

  private def listAllFields(): Future[Map[String, Seq[FieldModel]]] = {
    db.run(sql"select TABLE_NAME, COLUMN_NAME, DATA_TYPE from information_schema.columns where table_schema = DATABASE();".as[(String, String, String)])
      .map(_.groupBy(_._1).mapValues(_.map(x => parseToFiledModel(x._2 -> x._3)).toSeq))
  }

  private def makeTablesWithFields(tableList: Seq[TableModel], tableFieldMap: Map[String, Seq[FieldModel]]): Seq[FullTableModel] = {
    tableList.map(table => FullTableModel(table.name, table.isView, tableFieldMap(table.name)))
  }

  private def parseToFiledModel(f: (String, String)): FieldModel = {
    FieldModel(f._1, f._2)
  }

  private def parseToFiledModel(f: (String, String, String, String, String, String)): FieldModel = {
    FieldModel(f._1, f._2)
  }

  private def parseToTableModel(t: (String, String)): TableModel = {
    TableModel(t._1, isTableTypeView(t._2))
  }

  private def isTableTypeView(tableType: String): Boolean = tableType match {
    case "VIEW" => true
    case _      => false
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

  def apply(config: MySqlConnectionConfig)(executor: AsyncExecutor)(implicit executionContext: ExecutionContext): ConnectorResponse[MySqlConnector] = {

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
