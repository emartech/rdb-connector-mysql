package com.emarsys.rdb.connector.mysql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.SqlSyntaxError
import com.emarsys.rdb.connector.mysql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.RawSelectItSpec
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.{ExecutionContextExecutor}

class MySqlRawSelectItSpec
    extends TestKit(ActorSystem())
    with RawSelectItSpec
    with SelectDbInitHelper
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

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

  override val simpleSelect            = s"SELECT * FROM `$aTableName`;"
  override val badSimpleSelect         = s"SELECT * ForM `$aTableName`"
  override val simpleSelectNoSemicolon = s"""SELECT * FROM `$aTableName`"""
  val missingColumnSelect              = s"SELECT nope FROM $aTableName"

  "#analyzeRawSelect" should {
    "return result" in {
      val result = getStreamResult(connector.analyzeRawSelect(simpleSelect))

      result shouldEqual Seq(
        Seq(
          "id",
          "select_type",
          "table",
          "partitions",
          "type",
          "possible_keys",
          "key",
          "key_len",
          "ref",
          "rows",
          "filtered",
          "Extra"
        ),
        Seq(
          "1",
          "SIMPLE",
          s"$aTableName",
          null,
          "index",
          null,
          s"${aTableName.dropRight(5)}_idx2",
          "7",
          null,
          "7",
          "100.00",
          "Using index"
        )
      )
    }
  }

  "#rawSelect" when {
    "there is a syntax error in the query" should {
      "return SqlSyntaxError" in {
        val result = connector.rawSelect(missingColumnSelect, None)

        a[SqlSyntaxError] should be thrownBy {
          getStreamResult(result)
        }
      }
    }
  }

}
