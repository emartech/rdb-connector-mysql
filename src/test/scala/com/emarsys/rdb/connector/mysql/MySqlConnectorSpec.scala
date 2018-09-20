package com.emarsys.rdb.connector.mysql

import java.lang.management.ManagementFactory
import java.util.UUID

import com.emarsys.rdb.connector.common.models.Errors.ConnectionTimeout
import com.emarsys.rdb.connector.common.models.MetaData
import com.emarsys.rdb.connector.mysql.MySqlConnector.MySqlConnectionConfig
import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException
import com.zaxxer.hikari.HikariPoolMXBean
import javax.management.{MBeanServer, ObjectName}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpecLike}
import spray.json._
import slick.jdbc.MySQLProfile.api._

class MySqlConnectorSpec extends WordSpecLike with Matchers with MockitoSugar {

  "MySqlConnectorSpec" when {

    val exampleConnection = MySqlConnectionConfig(
      host = "host",
      port = 123,
      dbName = "database",
      dbUser = "me",
      dbPassword = "secret",
      certificate = "cert",
      connectionParams = "?param1=asd"
    )

    "#isErrorRetryable" should {

      Seq(
        new MySQLTransactionRollbackException() -> true,
        ConnectionTimeout("timeout")            -> true,
        new Exception()                         -> false
      ).foreach {
        case (e, expected) =>
          s"return $expected for ${e.getClass.getSimpleName}" in {
            val connector = new MySqlConnector(null, null, null)(null)

            connector.isErrorRetryable(e) shouldBe expected
          }
      }

    }

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

    "#meta" should {

      "return mysql qouters" in {
        MySqlConnector.meta() shouldEqual MetaData(nameQuoter = "`", valueQuoter = "'", escape = "\\")
      }

    }

    "#innerMetrics" should {

      implicit val executionContext = concurrent.ExecutionContext.Implicits.global

      "return Json in happy case" in {
        val mxPool = new HikariPoolMXBean {
          override def resumePool(): Unit = ???

          override def softEvictConnections(): Unit = ???

          override def getActiveConnections: Int = 4

          override def getThreadsAwaitingConnection: Int = 3

          override def suspendPool(): Unit = ???

          override def getTotalConnections: Int = 2

          override def getIdleConnections: Int = 1
        }

        val poolName = UUID.randomUUID.toString
        val db       = mock[Database]

        val mbs: MBeanServer      = ManagementFactory.getPlatformMBeanServer()
        val mBeanName: ObjectName = new ObjectName(s"com.zaxxer.hikari:type=Pool ($poolName)")
        mbs.registerMBean(mxPool, mBeanName)

        val connector   = new MySqlConnector(db, MySqlConnector.defaultConfig, poolName)
        val metricsJson = connector.innerMetrics().parseJson.asJsObject

        metricsJson.fields.size shouldEqual 4
        metricsJson.fields("totalConnections") shouldEqual JsNumber(2)
      }

      "return Json in sad case" in {
        val db          = mock[Database]
        val poolName    = ""
        val connector   = new MySqlConnector(db, MySqlConnector.defaultConfig, poolName)
        val metricsJson = connector.innerMetrics().parseJson.asJsObject
        metricsJson.fields.size shouldEqual 0
      }

    }

  }
}
