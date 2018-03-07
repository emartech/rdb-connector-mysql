package com.emarsys.rdb.connector.mysql

import com.emarsys.rdb.connector.common.defaults.{DefaultFieldValueConverters, FieldValueConverter}
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper
import com.emarsys.rdb.connector.common.models.SimpleSelect.Value

trait MysqlFieldValueConverters extends DefaultFieldValueConverters {
  override implicit val booleanValueConverter: FieldValueConverter[FieldValueWrapper.BooleanValue] = {
    case FieldValueWrapper.BooleanValue(true)  => Some(Value("1"))
    case FieldValueWrapper.BooleanValue(false) => Some(Value("0"))
  }
}

object MysqlFieldValueConverters extends MysqlFieldValueConverters
