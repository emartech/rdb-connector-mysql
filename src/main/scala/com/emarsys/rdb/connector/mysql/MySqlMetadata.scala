package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel, TableModel}
import com.emarsys.rdb.connector.mysql.Sanitizer.quoteIdentifier
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Future

trait MySqlMetadata {
  self: MySqlConnector =>

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
    case _ => false
  }
}

