package com.emarsys.rdb.connector.mysql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.mysql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.UpdateItSpec

class MySqlUpdateItSpec extends TestKit(ActorSystem()) with UpdateItSpec with SelectDbInitHelper {
  val aTableName: String = tableName
  val bTableName: String = "temp"

  override implicit val materializer: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

}