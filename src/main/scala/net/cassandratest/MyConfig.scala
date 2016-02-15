package net.cassandratest

import java.io.File

import com.typesafe.config.ConfigFactory

object MyConfig {



  val default = ConfigFactory.load()
  val config = ConfigFactory.parseFile(new File("test.conf")).withFallback(default)

  def chunkSize = config.getInt("test.settings.chunkSize")

  def bucketSize = config.getInt("test.settings.bucketsize")

  def maxFutures = config.getInt("test.settings.maxfutures")

  def readbufferSize = config.getInt("test.settings.readbuffersize")

  def serverName = config.getString("test.settings.servername")
  def fileName = config.getString("test.settings.filename")

  def tokenAwareDriver = config.getBoolean("test.settings.tokenawaredriver")

  def connectionNumber = config.getInt("test.settings.connectionNumber")

  def maxConnectionNumber = config.getInt("test.settings.maxConnectionNumber")

  def maxRequestsPerConnection: Int = config.getInt("test.settings.maxRequestsPerConnection")

  def cycles = config.getInt("test.settings.cycles")
  

  // unused, yet






}

