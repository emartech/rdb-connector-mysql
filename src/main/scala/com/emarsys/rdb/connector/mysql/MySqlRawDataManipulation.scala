package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.DataManipulation
import com.emarsys.rdb.connector.common.models.DataManipulation.{FieldValueWrapper, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.defaults.DefaultFieldValueWrapperConverter._
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

        val wherePart = createConditionQueryPart(definition.search)

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
    if(definitions.isEmpty) {
      Future.successful(Right(0))
    } else {
      val table = TableName(tableName).toSql

      val fields = definitions.head.keySet.toSeq
      val fieldList = fields.map(FieldName(_).toSql).mkString("(",",",")")
      val valueList = makeSqlValueList(orderValues(definitions, fields))

      val query = sqlu"INSERT IGNORE INTO #$table #$fieldList VALUES #$valueList"

      db.run(query)
        .map(result => Right(result))
        .recover {
          case ex => Left(ErrorWithMessage(ex.toString))
        }
    }
  }

  private def orderValues(data: Seq[Record], orderReference: Seq[String]): Seq[Seq[FieldValueWrapper]] = {
    data.map(row => orderReference.map(d => row.getOrElse(d, NullValue)))
  }

  private def makeSqlValueList(data: Seq[Seq[FieldValueWrapper]]) = {
    data.map(list =>
      list.map { d =>
        if (d == NullValue) {
          "NULL"
        } else {
          Value(convertTypesToString(d)).toSql
        }
      }   .mkString(", ")
    ).mkString("(","),(",")")
  }

  private def createConditionQueryPart(criteria: Map[String, FieldValueWrapper]) = {
    And(criteria.map {
      case (field, value) =>
        val strVal = convertTypesToString(value)
        if (strVal == null) {
          IsNull(FieldName(field))
        } else {
          EqualToValue(FieldName(field), Value(strVal))
        }
    }.toList).toSql
  }

  private def createSetQueryPart(criteria: Map[String, FieldValueWrapper]) = {
    criteria.map {
      case (field, value) =>
        val strVal = convertTypesToString(value)
        if (strVal == null) {
          FieldName(field).toSql + "=NULL"
        } else {
          EqualToValue(FieldName(field), Value(strVal)).toSql
        }
    }.mkString(", ")
  }

}