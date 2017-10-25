package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.mysql.MySqlConnector.MySqlConnectionConfig
import org.scalatest.{Matchers, WordSpecLike}

class MySqlConnectorSpec extends WordSpecLike with Matchers {

  "MySqlConnectorSpec" when {

    val exampleConnection = MySqlConnectionConfig(
      host = "host",
      port = 123,
      dbName = "database",
      user = "me",
      password = "secret",
      certificate = "cert",
      connectionParams = "?param1=asd"
    )

    "#createUrl" should {

      "creates url from config" in {
        MySqlConnector.createUrl(exampleConnection) shouldBe "jdbc:mysql://host:123/database?param1=asd"
      }

      "handle missing ? in params" in {
        val exampleWithoutMark = exampleConnection.copy(connectionParams = "param1=asd")
        MySqlConnector.createUrl(exampleWithoutMark) shouldBe "jdbc:mysql://host:123/database?param1=asd"
      }

      "handle empty params" in {
        val exampleWithoutMark = exampleConnection.copy(connectionParams = "")
        MySqlConnector.createUrl(exampleWithoutMark) shouldBe "jdbc:mysql://host:123/database"
      }
    }

  }
}
