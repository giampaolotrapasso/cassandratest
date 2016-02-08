package net.cassandratest

import java.io.File

import com.typesafe.config.ConfigFactory

object MyConfig {

  val default = ConfigFactory.load()
  val config = ConfigFactory.parseFile(new File("test.conf")).withFallback(default)

  def blobSize = config.getInt("test.settings.blobsize")

  def bucketSize = config.getInt("test.settings.bucketsize")

  def maxFutures = config.getInt("test.settings.maxfutures")

  def serverName = config.getString("test.settings.servername")

  def recordNumber = config.getInt("test.settings.recordnumber")
}

