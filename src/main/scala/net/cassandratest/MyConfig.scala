package net.cassandratest

import java.io.File

import com.typesafe.config.ConfigFactory

object MyConfig {



  val default = ConfigFactory.load()
  val config = ConfigFactory.parseFile(new File("test.conf")).withFallback(default)

  lazy val chunkSize = config.getInt("test.settings.chunkSize")

  lazy val bucketSize = config.getInt("test.settings.bucketsize")

  lazy val  maxFutures = config.getInt("test.settings.maxfutures")

  lazy val  readbufferSize = config.getInt("test.settings.readbuffersize")

  lazy val  serverName = config.getString("test.settings.servername")
  lazy val  fileName = config.getString("test.settings.filename")

  lazy val  tokenAwareDriver = config.getBoolean("test.settings.tokenawaredriver")

  lazy val  connectionNumber = config.getInt("test.settings.connectionNumber")

  lazy val  maxConnectionNumber = config.getInt("test.settings.maxConnectionNumber")

  lazy val  maxRequestsPerConnection: Int = config.getInt("test.settings.maxRequestsPerConnection")

  lazy val  cycles = config.getInt("test.settings.cycles")
  lazy val  poolsize = config.getInt("test.settings.poolsize")

  lazy val  keyspace = config.getString("test.settings.keyspace")

  lazy val  tableName = config.getString("test.settings.tablename")


  // unused, yet






}

