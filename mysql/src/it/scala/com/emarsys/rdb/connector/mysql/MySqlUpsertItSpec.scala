package com.emarsys.rdb.connector.mysql

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{IntValue, NullValue, StringValue}
import com.emarsys.rdb.connector.common.models.DataManipulation.Record
import com.emarsys.rdb.connector.common.models.Errors.FailedValidation
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.ValidateDataManipulation.ValidationResult.NonExistingFields
import com.emarsys.rdb.connector.mysql.utils.{SelectDbInitHelper, TestHelper}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class MySqlUpsertItSpec
    extends TestKit(ActorSystem())
    with SelectDbInitHelper
    with WordSpecLike
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  val uuid      = UUID.randomUUID().toString.replace("-", "")
  val tableName = s"upsert_tables_table_$uuid"

  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"
  val xTableName: String = s"xTableName"

  implicit val materializer: Materializer = ActorMaterializer()

  val awaitTimeout = 5.seconds
  val queryTimeout = 5.seconds

  override def afterAll(): Unit = {
    system.terminate()
    connector.close()
  }

  override def beforeEach(): Unit = {
    initDb()
  }

  override def afterEach(): Unit = {
    cleanUpDb()
  }

  override def initDb(): Unit = {
    val createXTableSql =
      s"""CREATE TABLE `$xTableName` (
				 |    A1 varchar(255) NOT NULL,
				 |    A2 int DEFAULT 2,
				 |    A3 tinyint(1),
				 |    PRIMARY KEY (A1)
				 |);""".stripMargin

    super.initDb()
    Await.result(TestHelper.executeQuery(createXTableSql), 5 seconds)
  }

  override def cleanUpDb(): Unit = {
    val dropXTableSql = s"""DROP TABLE `$xTableName`;"""
    super.cleanUpDb()
    Await.result(TestHelper.executeQuery(dropXTableSql), 5 seconds)
  }

  s"UpsertSpec $uuid" when {

    "#upsert" should {

      "validation error" in {

        val upsertNonExistingFieldFieldData: Seq[Record] =
          Seq(Map("a" -> StringValue("1")), Map("a" -> StringValue("2")))

        Await.result(connector.upsert(tableName, upsertNonExistingFieldFieldData), awaitTimeout) shouldBe Left(
          FailedValidation(NonExistingFields(Set("a")))
        )
      }

      "upsert successfully more records" in {

        val simpleSelectV1 = SimpleSelect(
          AllField,
          TableName(tableName),
          where = Some(
            And(
              Seq(
                EqualToValue(FieldName("A1"), Value("v1")),
                IsNull(FieldName("A2")),
                IsNull(FieldName("A3"))
              )
            )
          )
        )

        val simpleSelectV1new = SimpleSelect(
          AllField,
          TableName(tableName),
          where = Some(
            And(
              Seq(
                EqualToValue(FieldName("A1"), Value("v1new")),
                EqualToValue(FieldName("A2"), Value("777")),
                IsNull(FieldName("A3"))
              )
            )
          )
        )

        val simpleSelectAll = SimpleSelect(AllField, TableName(tableName))

        val insertAndUpdateData: Seq[Record] =
          Seq(
            Map("A1" -> StringValue("v1"), "A2"    -> NullValue, "A3"     -> NullValue),
            Map("A1" -> StringValue("v1new"), "A2" -> IntValue(777), "A3" -> NullValue)
          )

        Await.result(connector.upsert(tableName, insertAndUpdateData), awaitTimeout) shouldBe Right(2)
        Await
          .result(connector.simpleSelect(simpleSelectAll, queryTimeout), awaitTimeout)
          .map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(8 + 1)
        Await
          .result(connector.simpleSelect(simpleSelectV1, queryTimeout), awaitTimeout)
          .map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(1 + 1)
        Await
          .result(connector.simpleSelect(simpleSelectV1new, queryTimeout), awaitTimeout)
          .map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(1 + 1)
      }

      "upsert successfully only given fields" in {

        val insertRow1: Seq[Record] = Seq(
          Map("A1" -> StringValue("v1"), "A3" -> IntValue(2))
        )

        val insertRow2: Seq[Record] = Seq(
          Map("A1" -> StringValue("v2"), "A2" -> IntValue(9), "A3" -> IntValue(23))
        )

        val updateRow1: Seq[Record] = Seq(
          Map("A1" -> StringValue("v1"))
        )

        val updateRow2: Seq[Record] = Seq(
          Map("A1" -> StringValue("v2"), "A2" -> IntValue(777))
        )

        Await.result(connector.upsert(xTableName, insertRow1), awaitTimeout) shouldBe Right(1)
        Await.result(connector.upsert(xTableName, insertRow2), awaitTimeout) shouldBe Right(1)
        selectAll(xTableName) shouldBe Right(
          Seq(
            Seq("v1", "2", "2"),
            Seq("v2", "9", "23")
          )
        )

        Await.result(connector.upsert(xTableName, updateRow1), awaitTimeout) shouldBe Right(1)
        Await.result(connector.upsert(xTableName, updateRow2), awaitTimeout) shouldBe Right(1)
        selectAll(xTableName) shouldBe Right(
          Seq(
            Seq("v1", "2", "2"),
            Seq("v2", "777", "23")
          )
        )
      }

    }
  }

  private def selectAll(tableName: String) = {
    Await
      .result(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName)), queryTimeout), awaitTimeout)
      .map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).drop(1))
  }
}
