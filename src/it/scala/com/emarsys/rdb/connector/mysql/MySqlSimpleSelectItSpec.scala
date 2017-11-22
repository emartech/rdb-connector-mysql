package com.emarsys.rdb.connector.mysql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.mysql.util.SelectDbInitHelper
import com.emarsys.rdb.connector.test.SimpleSelectItSpec

class MySqlSimpleSelectItSpec extends TestKit(ActorSystem()) with SimpleSelectItSpec with SelectDbInitHelper {

  override implicit val materializer: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

}
