package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import org.scalatest.{Matchers, WordSpecLike}
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import MySqlWriters._

class MySqlWritersSpec extends WordSpecLike with Matchers {
  "MySqlWriters" when {

    "SimpleSelect" should {
      
      "use mysql writer - full" in {
        
        val select = SimpleSelect(
          fields = SpecificFields(Seq(FieldName("""FI`E'L\D1"""), FieldName("FIELD2"), FieldName("FIELD3"))),
          table = TableName("TABLE1"),
          where = Some(And(Seq(IsNull(FieldName("FIELD1")), And(Seq(IsNull(FieldName("FIELD2")), EqualToValue(FieldName("FIELD3"), Value("VALUE3"))))))),
          limit = Some(100)
        )

        select.toSql shouldEqual """SELECT `FI\`E'L\\D1`,`FIELD2`,`FIELD3` FROM `TABLE1` WHERE (`FIELD1` IS NULL AND (`FIELD2` IS NULL AND `FIELD3`='VALUE3')) LIMIT 100"""
      }
    }
  }
}
