package com.emarsys.rdb.connector.mysql

import org.scalatest.{Matchers, WordSpec}


class SanitizerSpec extends WordSpec with Matchers {

  "Sanitizer" when {

    ".quoteIdentifier" should {

      "surround simple text with backtick" in {
        val originalText: String = "simple text"
        val expectedText: String = "`simple text`"

        Sanitizer.quoteIdentifier(originalText) shouldEqual expectedText
      }

      "escape backticks with backtick" in {
        val originalText: String = """asd`asd"""
        val expectedText: String = """`asd``asd`"""

        Sanitizer.quoteIdentifier(originalText) shouldEqual expectedText
      }

      "removes backslashes" in {
        val originalText: String = """a\s\d\`\a\\\s\\d"""
        val expectedText: String = """`asd``asd`"""

        Sanitizer.quoteIdentifier(originalText) shouldEqual expectedText
      }

      "concats dbName and tableName with period inbetween them after sanitizing" in {
        val dbName: String = """a\s\d\`\a\\\s\\d"""
        val tableName: String = """table\Name"""
        val expectedText: String = """`asd``asd`.`tableName`"""

        Sanitizer.quoteIdentifier(dbName, tableName) shouldEqual expectedText
      }

    }

  }

}
