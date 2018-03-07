package com.emarsys.rdb.connector.mysql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.mysql.utils.TestHelper
import com.emarsys.rdb.connector.test.SearchItSpec
import slick.util.AsyncExecutor

import scala.concurrent.Await
import scala.concurrent.duration._

class MySqlSearchItSpec extends TestKit(ActorSystem()) with SearchItSpec {
  import scala.concurrent.ExecutionContext.Implicits.global

  val connector: Connector = Await.result(MySqlConnector(TestHelper.TEST_CONNECTION_CONFIG)(AsyncExecutor.default()), 5.seconds).right.get

  override implicit val materializer: Materializer = ActorMaterializer()

  override val awaitTimeout = 15.seconds

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  def initDb(): Unit = {
    val createZTableSql =
      s"""CREATE TABLE `$tableName` (
         |    z1 varchar(255) NOT NULL,
         |    z2 int,
         |    z3 tinyint(1),
         |    z4 varchar(255),
         |    PRIMARY KEY (z1)
         |);""".stripMargin

    val insertZDataSql =
      s"""INSERT INTO `$tableName` (z1,z2,z3,z4) VALUES
         |  ('r1', 1, 1, 's1'),
         |  ('r2', 2, 0, 's2'),
         |  ('r3', 3, NULL, 's3'),
         |  ('r4', 45, 1, 's4'),
         |  ('r5', 45, 1, 's5')
         |;""".stripMargin

    val addIndex1 =
      s"""CREATE INDEX `${tableName.dropRight(10)}_idx1` ON `$tableName` (z2);"""

    val addIndex2 =
      s"""CREATE INDEX `${tableName.dropRight(5)}_idx2` ON `$tableName` (z3);"""

    Await.result(for {
      _ <- TestHelper.executeQuery(createZTableSql)
      _ <- TestHelper.executeQuery(insertZDataSql)
      _ <- TestHelper.executeQuery(addIndex1)
      _ <- TestHelper.executeQuery(addIndex2)
    } yield (), 15.seconds)
  }

  def cleanUpDb(): Unit = {
    val dropZTableSql = s"""DROP TABLE `$tableName`;"""
    Await.result(for {
      _ <- TestHelper.executeQuery(dropZTableSql)
    } yield (), 15.seconds)
  }
}