package com.emarsys.rdb.connector.mysql

import com.typesafe.config.ConfigFactory

import scala.util.Try

trait Config {

  private val config = ConfigFactory.load()

  object db {
    lazy val useSsl = Try(config.getBoolean("mysqldb.use-ssl")).getOrElse(true)
    lazy val useHikari = Try(config.getBoolean("mysqldb.use-hikari")).getOrElse(false)
  }

}

object Config extends Config