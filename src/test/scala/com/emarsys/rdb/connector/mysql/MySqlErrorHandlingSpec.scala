package com.emarsys.rdb.connector.mysql

import java.sql.SQLTransientConnectionException

import com.emarsys.rdb.connector.common.models.Errors.{ConnectionError, ConnectionTimeout, QueryTimeout}
import com.mysql.jdbc.exceptions.MySQLTimeoutException
import org.scalatest.{Matchers, WordSpecLike}

class MySqlErrorHandlingSpec extends WordSpecLike with Matchers {

  "MySqlErrorHandling" should {

    "convert timeout transient sql error to connection timeout error" in new MySqlErrorHandling {
      val msg = "Connection is not available, request timed out after"
      val e   = new SQLTransientConnectionException(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(ConnectionTimeout(msg))
    }

    "convert transient sql error to connection error if not timeout" in new MySqlErrorHandling {
      val msg = "Other transient error"
      val e   = new SQLTransientConnectionException(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(ConnectionError(e))
    }

    "convert timeout error to query timeout error if query is cancelled" in new MySqlErrorHandling {
      val msg = "Statement cancelled due to timeout or client request"
      val e   = new MySQLTimeoutException(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(QueryTimeout(msg))
    }

    "convert timeout error to connection error if query is not cancelled" in new MySqlErrorHandling {
      val msg = "Other timeout error"
      val e   = new MySQLTimeoutException(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(ConnectionTimeout(msg))
    }

  }

}
