package com.emarsys.rdb.connector.mysql

import java.sql.SQLTransientConnectionException
import java.util.concurrent.RejectedExecutionException

import com.emarsys.rdb.connector.common.models.Errors
import com.emarsys.rdb.connector.common.models.Errors._
import com.mysql.jdbc.exceptions.MySQLTimeoutException
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException
import org.scalatest.{Matchers, WordSpecLike}

class MySqlErrorHandlingSpec extends WordSpecLike with Matchers {

  "MySqlErrorHandling" should {

    "convert timeout transient sql error to connection timeout error" in new MySqlErrorHandling {
      val msg = "Connection is not available, request timed out after"
      val e   = new SQLTransientConnectionException(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(ConnectionTimeout(msg))
    }

    "convert sql error to error with message and state if not timeout" in new MySqlErrorHandling {
      val msg = "Other transient error"
      val e   = new SQLTransientConnectionException(msg, "not-handled-sql-state")
      eitherErrorHandler.apply(e) shouldEqual Left(Errors.ErrorWithMessage(s"[not-handled-sql-state] - $msg"))
    }

    "convert general error to error with only message if not timeout" in new MySqlErrorHandling {
      val msg = "Other transient error"
      val e   = new Exception(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(Errors.ErrorWithMessage(msg))
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

    "convert syntax error exception to access denied error if the message implies that" in new MySqlErrorHandling {
      val msg = "Access denied; you need (at least one of) the PROCESS privilege(s) for this operation"
      val e   = new MySQLSyntaxErrorException(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(AccessDeniedError(msg))
    }

    "convert syntax error exception to SyntaxError" in new MySqlErrorHandling {
      val msg = "You have an error in your MySql syntax"
      val e   = new MySQLSyntaxErrorException(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(SqlSyntaxError(msg))
    }

    "convert RejectedExecutionException to TooManyQueries" in new MySqlErrorHandling {
      val msg = "There were too many queries"
      val e   = new RejectedExecutionException(msg)
      eitherErrorHandler.apply(e) shouldEqual Left(TooManyQueries(msg))
    }

  }

}
