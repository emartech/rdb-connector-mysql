package com.emarsys.rdb.connector.mysql

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.mysql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.InsertItSpec

import scala.concurrent.Await

class MySqlInsertSpec extends TestKit(ActorSystem()) with InsertItSpec with SelectDbInitHelper {

  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override implicit val materializer: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  val simpleSelectExisting = SimpleSelect(
    AllField,
    TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("v1"))
    )
  )

  s"InsertIgnoreSpec $uuid" when {

    "#insertIgnore" should {

      "ignore if inserting existing record" in {
        Await.result(connector.insertIgnore(tableName, insertExistingData), awaitTimeout) shouldBe Right(0)
        Await
          .result(connector.simpleSelect(simpleSelectAllWithExpectedResultSize(8), queryTimeout), awaitTimeout)
          .map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(8)
        Await
          .result(connector.simpleSelect(simpleSelectExisting, queryTimeout), awaitTimeout)
          .map(stream => Await.result(stream.runWith(Sink.seq), awaitTimeout).size) shouldBe Right(2)
      }
    }
  }
}
