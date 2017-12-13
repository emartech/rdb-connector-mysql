package com.emarsys.rdb.connector.mysql

import com.typesafe.config.ConfigFactory

import scala.util.Try

trait Config {

  private val config = ConfigFactory.load()

  object db {
    lazy val useSsl = Try(config.getBoolean("db.use-ssl")).getOrElse(true)
  }

}

object Config extends Config