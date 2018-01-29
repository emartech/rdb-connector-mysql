package com.emarsys.rdb.connector.mysql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.mysql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.RawSelectItSpec
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.{Await, ExecutionContextExecutor}

class MySqlRawSelectItSpec extends TestKit(ActorSystem()) with RawSelectItSpec with SelectDbInitHelper with WordSpecLike with Matchers with BeforeAndAfterAll{

  implicit val materializer: Materializer = ActorMaterializer()

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override def afterAll(): Unit = {
    system.terminate()
    cleanUpDb()
    connector.close()
  }

  override def beforeAll(): Unit = {
    initDb()
  }

  override val simpleSelect = s"SELECT * FROM `$aTableName`;"
  override val badSimpleSelect = s"SELECT * ForM `$aTableName`"
  override val simpleSelectNoSemicolon = s"""SELECT * FROM `$aTableName`"""

  "#validateProjectedRawSelect" should {
    "return ok if ok" in {
      Await.result(connector.validateProjectedRawSelect(simpleSelect, Seq("A1")), awaitTimeout) shouldBe Right()
    }

    "return ok if no ; in query" in {
      Await.result(connector.validateProjectedRawSelect(simpleSelectNoSemicolon, Seq("A1")), awaitTimeout) shouldBe Right()
    }

    "return error if not ok" in {
      Await.result(connector.validateProjectedRawSelect(simpleSelect, Seq("NONEXISTENT_COLUMN")), awaitTimeout) shouldBe a[Left[ErrorWithMessage, Unit]]
    }
  }

  "#analyzeRawSelect" should {
    "return result" in {
      val result = getStreamResult(connector.analyzeRawSelect(simpleSelect))

      result shouldEqual Seq(
        Seq("id", "select_type", "table", "partitions", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "Extra"),
        Seq("1", "SIMPLE", s"$aTableName", null, "index", null, s"${aTableName.dropRight(5)}_idx2", "7", null, "7", "100.00", "Using index")
      )
    }
  }
}
