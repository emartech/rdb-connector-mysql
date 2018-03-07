package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, FieldValueWrapper, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.NullValue
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.mysql.MySqlWriters._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Future

trait MySqlRawDataManipulation {
  self: MySqlConnector =>

  override def rawUpdate(tableName: String, definitions: Seq[UpdateDefinition]): ConnectorResponse[Int] = {
    if(definitions.isEmpty) {
      Future.successful(Right(0))
    } else {
      val table = TableName(tableName).toSql
      val queries = definitions.map { definition =>

        val setPart = createSetQueryPart(definition.update)

        val wherePart = createConditionQueryPart(definition.search).toSql

        sqlu"UPDATE #$table SET #$setPart WHERE #$wherePart"
      }

      db.run(DBIO.sequence(queries).transactionally)
        .map(results => Right(results.sum))
        .recover {
          case ex => Left(ErrorWithMessage(ex.toString))
        }
    }
  }

  override def rawInsertData(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = {
    rawXsert("INSERT IGNORE", tableName, definitions)
  }

  override def rawUpsert(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = {
    rawXsert("REPLACE", tableName, definitions).map(_.map(_ => definitions.size))
  }

  private def rawXsert(method: String, tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = {
    if(definitions.isEmpty) {
      Future.successful(Right(0))
    } else {
      val table = TableName(tableName).toSql

      val fields = definitions.head.keySet.toSeq
      val fieldList = fields.map(FieldName(_).toSql).mkString("(",",",")")
      val valueList = makeSqlValueList(orderValues(definitions, fields))

      val query = sqlu"#$method INTO #$table #$fieldList VALUES #$valueList"

      db.run(query)
        .map(result => Right(result))
        .recover {
          case ex => Left(ErrorWithMessage(ex.toString))
        }
    }
  }

  override def rawDelete(tableName: String, criteria: Seq[Criteria]): ConnectorResponse[Int] = {
    if (criteria.isEmpty) {
      Future.successful(Right(0))
    } else {
      val table = TableName(tableName).toSql
      val condition = Or(criteria.map(createConditionQueryPart)).toSql
      val query = sqlu"DELETE FROM #$table WHERE #$condition"

      db.run(query)
        .map(result => Right(result))
        .recover {
          case ex => Left(ErrorWithMessage(ex.toString))
        }
    }
  }

  override def rawReplaceData(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = {
    val newTableName = generateTempTableName(tableName)
    val newTable = TableName(newTableName).toSql
    val table = TableName(tableName).toSql
    val createTableQuery = sqlu"CREATE TABLE #$newTable LIKE #$table"
    val dropTableQuery = sqlu"DROP TABLE IF EXISTS #$newTable"

    db.run(createTableQuery)
      .flatMap( _ =>
        rawInsertData(newTableName, definitions).flatMap( insertedCount =>
          swapTableNames(tableName, newTableName).flatMap( _ =>
            db.run(dropTableQuery).map( _ => insertedCount )
          )
        )
      )
      .recover {
        case ex => Left(ErrorWithMessage(ex.toString))
      }
  }

  private def swapTableNames(tableName: String, newTableName: String): Future[Int] = {
    val temporaryTableName = generateTempTableName()
    val tablePairs =  Seq((tableName, temporaryTableName), (newTableName, tableName), (temporaryTableName, newTableName))
    val commaSeparatedRenames = tablePairs.map({case (from, to) => TableName(from).toSql + " TO " + TableName(to).toSql }).mkString(", ")
    val query = sqlu"RENAME TABLE #$commaSeparatedRenames"
    db.run(query)
  }

  private def generateTempTableName(original: String = ""): String = {
    val shortedName = if(original.length > 30) original.take(30) else original
    val id = java.util.UUID.randomUUID().toString.replace("-","").take(30)
    shortedName + "_" + id
  }

  private def orderValues(data: Seq[Record], orderReference: Seq[String]): Seq[Seq[FieldValueWrapper]] = {
    data.map(row => orderReference.map(d => row.getOrElse(d, NullValue)))
  }

  private def makeSqlValueList(data: Seq[Seq[FieldValueWrapper]]) = {
    import com.emarsys.rdb.connector.common.defaults.FieldValueConverter._
    import fieldValueConverters._

    data.map(_.map(_.toSimpleSelectValue.map(_.toSql).getOrElse("NULL")).mkString(", "))
      .mkString("(", "),(", ")")
  }

  private def createConditionQueryPart(criteria: Criteria) = {
    import com.emarsys.rdb.connector.common.defaults.FieldValueConverter._
    import fieldValueConverters._

    And(criteria.mapValues(_.toSimpleSelectValue).map {
      case (field, Some(value)) => EqualToValue(FieldName(field), value)
      case (field, None)        => IsNull(FieldName(field))
    }.toList)
  }

  private def createSetQueryPart(criteria: Map[String, FieldValueWrapper]) = {
    import com.emarsys.rdb.connector.common.defaults.FieldValueConverter._
    import fieldValueConverters._

    criteria.mapValues(_.toSimpleSelectValue).map {
      case (field, Some(value)) => EqualToValue(FieldName(field), value).toSql
      case (field, None)        => FieldName(field).toSql + "=NULL"
    }.mkString(", ")
  }


}
