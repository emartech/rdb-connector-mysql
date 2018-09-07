package com.emarsys.rdb.connector.mysql

import java.sql.SQLTransientConnectionException

import com.emarsys.rdb.connector.common.models.Errors.{ConnectionError, ConnectionTimeout}
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

  }

}
