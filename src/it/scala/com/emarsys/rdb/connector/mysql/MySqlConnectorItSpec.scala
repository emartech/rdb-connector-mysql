package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.mysql.utils.TestHelper
import org.scalatest.{Matchers, WordSpecLike}
import slick.util.AsyncExecutor

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class MySqlConnectorItSpec extends WordSpecLike with Matchers {

  "MySqlConnectorItSpec" when {

    val testConnection = TestHelper.TEST_CONNECTION_CONFIG

    "create connector" should {

      "connect success" in {
        val connectorEither = Await.result(MySqlConnector(testConnection)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a [Right[_, _]]

        connectorEither.right.get.close()
      }

      "connect fail when ssl disabled" in {
        val conn = testConnection.copy(
          connectionParams = "useSSL=false"
        )
        val connectorEither = Await.result(MySqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe Left(ErrorWithMessage("SSL Error"))
      }

      "connect ok when ssl disabled but check is disabled" in {
        val conn = testConnection.copy(
          connectionParams = "useSSL=false"
        )

        object MySqlConnWithoutSSL extends MySqlConnectorTrait {
          override val useSSL: Boolean = false
        }

        val connectorEither = Await.result(MySqlConnWithoutSSL(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a [Right[_, _]]
      }

      "connect fail when wrong certificate" in {
        val conn = testConnection.copy(certificate = "")
        val connectorEither = Await.result(MySqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe Left(ErrorWithMessage("Cannot connect to the sql server"))
      }

      "connect fail when wrong host" in {
        val conn = testConnection.copy(host = "wrong")
        val connectorEither = Await.result(MySqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe Left(ErrorWithMessage("Cannot connect to the sql server"))
      }

      "connect fail when wrong user" in {
        val conn = testConnection.copy(dbUser = "")
        val connectorEither = Await.result(MySqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe Left(ErrorWithMessage("Cannot connect to the sql server"))
      }

      "connect fail when wrong password" in {
        val conn = testConnection.copy(dbPassword = "")
        val connectorEither = Await.result(MySqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe Left(ErrorWithMessage("Cannot connect to the sql server"))
      }

    }

    "test connection" should {

      "success" in {
        val connectorEither = Await.result(MySqlConnector(testConnection)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a [Right[_, _]]
        
        val connector = connectorEither.right.get

        val result = Await.result(connector.testConnection(), 5.seconds)

        result shouldBe a [Right[_, _]]

        connector.close()
      }

    }

  }
}
