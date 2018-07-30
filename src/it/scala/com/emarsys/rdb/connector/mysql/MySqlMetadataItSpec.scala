package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel}
import com.emarsys.rdb.connector.mysql.utils.TestHelper
import com.emarsys.rdb.connector.test.MetadataItSpec
import slick.util.AsyncExecutor

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class MySqlMetadataItSpec extends MetadataItSpec {

  val connector: Connector = Await.result(MySqlConnector(TestHelper.TEST_CONNECTION_CONFIG)(AsyncExecutor.default()), 5.seconds).right.get

  def initDb(): Unit = {
    val createTableSql = s"""CREATE TABLE `$tableName` (
                           |    PersonID int,
                           |    LastName varchar(255),
                           |    FirstName varchar(255),
                           |    Address varchar(255),
                           |    City varchar(255)
                           |);""".stripMargin

    val createViewSql = s"""CREATE VIEW `$viewName` AS
                          |SELECT PersonID, LastName, FirstName
                          |FROM `$tableName`;""".stripMargin
    Await.result(for {
      _ <- TestHelper.executeQuery(createTableSql)
      _ <- TestHelper.executeQuery(createViewSql)
    } yield (), 5.seconds)
  }

  def cleanUpDb(): Unit = {
    val dropViewSql = s"""DROP VIEW `$viewName`;"""
    val dropTableSql = s"""DROP TABLE `$tableName`;"""
    val dropViewSql2 = s"""DROP VIEW IF EXISTS `${viewName}_2`;"""
    val dropTableSql2 = s"""DROP TABLE IF EXISTS `${tableName}_2`;"""
    Await.result(for {
      _ <- TestHelper.executeQuery(dropViewSql)
      _ <- TestHelper.executeQuery(dropTableSql)
      _ <- TestHelper.executeQuery(dropViewSql2)
      _ <- TestHelper.executeQuery(dropTableSql2)
    } yield (), 5.seconds)
  }

  s"MetadataItSpec $uuid" when {

    "#listTablesWithFields" should {
      "list without view after table dropped" in {
        val createTableSql = s"""CREATE TABLE `${tableName}_2` (
                                |    PersonID int,
                                |    LastName varchar(255),
                                |    FirstName varchar(255),
                                |    Address varchar(255),
                                |    City varchar(255)
                                |);""".stripMargin

        val createViewSql = s"""CREATE VIEW `${viewName}_2` AS
                               |SELECT PersonID, LastName, FirstName
                               |FROM `${tableName}_2`;""".stripMargin

        val dropTableSql = s"""DROP TABLE `${tableName}_2`;"""

        Await.result(for {
          _ <- TestHelper.executeQuery(createTableSql)
          _ <- TestHelper.executeQuery(createViewSql)
          _ <- TestHelper.executeQuery(dropTableSql)
        } yield (), 5.seconds)

        val tableFields = Seq("PersonID", "LastName", "FirstName", "Address", "City").map(_.toLowerCase()).sorted.map(FieldModel(_, ""))
        val viewFields = Seq("PersonID", "LastName", "FirstName").map(_.toLowerCase()).sorted.map(FieldModel(_, ""))

        val resultE = Await.result(connector.listTablesWithFields(), awaitTimeout)

        resultE shouldBe a[Right[_,_]]
        val result = resultE.right.get.map(x => x.copy(fields = x.fields.map(f => f.copy(name = f.name.toLowerCase, columnType = "")).sortBy(_.name)))
        result should not contain (FullTableModel(s"${tableName}_2", false, tableFields))
        result should not contain (FullTableModel(s"${viewName}_2", true, viewFields))
      }
    }
  }
}
