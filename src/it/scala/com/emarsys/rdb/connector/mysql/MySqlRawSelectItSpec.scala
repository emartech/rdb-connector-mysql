package com.emarsys.rdb.connector.mysql

import java.util.UUID

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.mysql.utils.SelectDbInitHelper
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import concurrent.duration._
import scala.concurrent.Await

class MySqlRawSelectItSpec extends TestKit(ActorSystem()) with SelectDbInitHelper with WordSpecLike with Matchers with BeforeAndAfterAll{

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val materializer: Materializer = ActorMaterializer()

  val uuid = UUID.randomUUID().toString

  val postfixTableName = s"_raw_select_table_$uuid"

  val aTableName = s"a$postfixTableName"
  val bTableName = s"b$postfixTableName"

  val awaitTimeout = 5.seconds

  override def afterAll(): Unit = {
    system.terminate()
    cleanUpDb()
    connector.close()
  }

  override def beforeAll(): Unit = {
    initDb()
  }

  s"RawSelectItSpec $uuid" when {

    "#rawSelect" should {
      "list table values" in {
        val simpleSelect = s"SELECT * FROM `$aTableName`;"

        val result = getRawSelectResult(simpleSelect, None)

        checkResultWithoutRowOrder(result, Seq(
          Seq("A1", "A2", "A3"),
          Seq("v1", "1", "1"),
          Seq("v2", "2", "0"),
          Seq("v3", "3", "1"),
          Seq("v4", "-4", "0"),
          Seq("v5", null, "0"),
          Seq("v6", "6", null),
          Seq("v7", null, null)
        ))
      }

      "list table values with limit" in {
        val simpleSelect = s"SELECT * FROM `$aTableName`;"

        val limit = 2

        val result = getRawSelectResult(simpleSelect, Option(limit))

        result.size shouldEqual limit+1
      }

      "fails the future if query is bad" in {
        val simpleSelect = s"SELECT * ForM `$aTableName`"

        assertThrows[Exception](Await.result(connector.rawSelect(simpleSelect, None).flatMap(_.right.get.runWith(Sink.seq)), awaitTimeout))
      }
    }

    "#validateRawSelect" should {
      "return ok if ok" in {
        val simpleSelect = s"SELECT * FROM `$aTableName`;"

        Await.result(connector.validateRawSelect(simpleSelect), awaitTimeout) shouldBe Right()
      }

      "return error if not ok" in {
        val simpleSelect = s"SELECT * ForM `$aTableName`"

        Await.result(connector.validateRawSelect(simpleSelect), awaitTimeout) shouldBe a [Left[ErrorWithMessage,Unit]]
      }
    }
  }

  def checkResultWithoutRowOrder(result: Seq[Seq[String]], expected: Seq[Seq[String]]): Unit = {
    result.size shouldEqual expected.size
    result.head.map(_.toUpperCase) shouldEqual expected.head.map(_.toUpperCase)
    result.foreach(expected contains _)
  }

  def getRawSelectResult(rawSql: String, limit: Option[Int]): Seq[Seq[String]] = {
    val resultE = Await.result(connector.rawSelect(rawSql, limit), awaitTimeout)

    resultE shouldBe a[Right[_, _]]
    val resultStream: Source[Seq[String], NotUsed] = resultE.right.get

    Await.result(resultStream.runWith(Sink.seq), awaitTimeout)
  }

}
