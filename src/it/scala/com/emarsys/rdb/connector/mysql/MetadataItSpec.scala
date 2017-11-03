package com.emarsys.rdb.connector.mysql

import java.util.UUID

import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.TableModel
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import slick.util.AsyncExecutor

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

trait MetadataItSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  val uuid = UUID.randomUUID().toString
  val tableName = s"metadata_list_tables_table_$uuid"
  val viewName = s"metadata_list_tables_view_$uuid"
  val connector: Connector

  override def beforeAll(): Unit = {
    initDb()
  }

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
  }

  def initDb(): Unit
  def cleanUpDb(): Unit

  s"MetadataItSpec $uuid" when {

    "#listTables" should {
      "list tables and views" in {
        val resultE = Await.result(connector.listTables(), 5.seconds)

        resultE shouldBe a[Right[_,_]]
        val result = resultE.right.get

        result should contain (TableModel(tableName, false))
        result should contain (TableModel(viewName, true))
      }
    }

    "#listFields" should {
      "list table fields" in {
        val resultE = Await.result(connector.listFields(tableName), 5.seconds)

        resultE shouldBe a[Right[_,_]]
        val result = resultE.right.get

        val fieldNames = result.map(_.name)

        fieldNames shouldEqual Seq("PersonID", "LastName", "FirstName", "Address", "City")
      }
    }
  }
}

class MysqlMetadataItSpec extends MetadataItSpec {

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
    Await.result(for {
      _ <- TestHelper.executeQuery(dropViewSql)
      _ <- TestHelper.executeQuery(dropTableSql)
    } yield (), 5.seconds)
  }

}
