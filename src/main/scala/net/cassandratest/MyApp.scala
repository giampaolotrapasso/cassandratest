package net.cassandratest

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import com.datastax.driver.core.{BatchStatement, Cluster, HostDistance, PoolingOptions, PreparedStatement, ProtocolVersion, ResultSet, Session}
import com.google.common.util.concurrent.{Futures, ListenableFuture, ListeningExecutorService, MoreExecutors}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future => SFuture}

class Database(node: String) extends LazyLogging {

  val poolingOptions = new PoolingOptions()
    .setConnectionsPerHost(
      HostDistance.LOCAL, MyConfig.connectionNumber, MyConfig.maxConnectionNumber)
    .setConnectionsPerHost(
      HostDistance.REMOTE, MyConfig.connectionNumber, MyConfig.maxConnectionNumber)
    .setMaxRequestsPerConnection(
      HostDistance.LOCAL, MyConfig.maxRequestsPerConnection)
    .setMaxRequestsPerConnection(
      HostDistance.REMOTE, MyConfig.maxRequestsPerConnection)
    .setPoolTimeoutMillis(200000)


  var clusterBuilder = Cluster.builder.addContactPoint(node)
    .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
    .withPoolingOptions(poolingOptions)
    .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)

  val cluster = clusterBuilder.build

  val session = cluster.newSession()
}

case class Record(uuid: UUID, bytes: Array[Byte], bucket: Int, start: Int, end: Int, offset: Int, length: Int)

case class FileInfo(size: Long, chunks: Int)

object MyApp extends LazyLogging with FutureHelper {

  var futures = ListBuffer[ListenableFuture[ResultSet]]()
  val client = new Database(MyConfig.serverName)
  var writeResults = List[Double]()
  var readResults = List[Double]()
  var allocationResults = List[Double]()
  var bindResult = List[Double]()
  var executeResult = List[Double]()
  var writeCounter = 0

  var bindTime = 0d
  var executeWithAsynchTime = 0d

  implicit val s = client.session

  var time = System.currentTimeMillis()

  //val threadPool = Executors.newFixedThreadPool(MyConfig.poolsize)

 // implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(threadPool)

  //implicit val executor: ListeningExecutorService = MoreExecutors.listeningDecorator(threadPool)


  private def getFileInfo(bigDataFilePath: String) = {
    val randomAccessFile = new RandomAccessFile(bigDataFilePath, "r")
    try {
      FileInfo(randomAccessFile.length,
        (randomAccessFile.length / MyConfig.readbufferSize.toDouble).ceil.toInt)
    } finally {
      randomAccessFile.close
    }
  }


  def main(args: Array[String]): Unit = {

    val tableName = MyConfig.tableName
    val keyspace = MyConfig.keyspace

    createSchema(client.session, keyspace, tableName)

    val statement = client.session.prepare(s"INSERT INTO $keyspace.$tableName(uuid, bucket, start, end, data) VALUES (?, ?, ?, ?, ?);")
    val fileInfo = getFileInfo(MyConfig.fileName)

    var fileSize = 0
    val readBufferSize = MyConfig.readbufferSize


    var i = 0;

    while (i < MyConfig.cycles) {
      writeCounter = 0
      time = System.currentTimeMillis
      val raf = new RandomAccessFile(MyConfig.fileName, "r")
      val uuid: UUID = UUID.randomUUID()
      var readTime = 0d
      var allocationTime = 0d
      try {
        val byteBuffer = new Array[Byte](readBufferSize)

        bindTime = 0
        executeWithAsynchTime = 0d

        for (chunkIndex <- 1 to fileInfo.chunks) {

          val seek = (chunkIndex - 1) * readBufferSize
          val timeStartRead = System.currentTimeMillis
          raf.seek(seek)
          val readSize = raf.read(byteBuffer)
          readTime += (System.currentTimeMillis - timeStartRead).toDouble

          val timeAllocationStarted = System.currentTimeMillis()
          write(uuid, client,
            bucketIndex = chunkIndex,
            bucketSize = MyConfig.bucketSize,
            chunkSize = MyConfig.chunkSize,
            byteBuffer.array,
            statement, readSize)
          allocationTime += System.currentTimeMillis() - timeAllocationStarted
        }
      }
      finally {
        raf.close
      }

      import scala.collection.JavaConversions._
      val list: ListenableFuture[util.List[ResultSet]] = Futures.allAsList(futures)

      list.get(60, TimeUnit.SECONDS)


      val writeTime = (System.currentTimeMillis - time).toDouble
      writeResults = writeResults :+ writeTime
      bindResult = bindResult :+ bindTime
      readResults = readResults :+ readTime
      allocationResults = allocationResults :+ allocationTime
      executeResult = executeResult :+ executeWithAsynchTime

      i += 1

    }
    val mbSize = fileInfo.size / 1000000.0
    val bufferMbSize = MyConfig.readbufferSize / 1000000.0

    val allocationSpeeds = allocationResults.map(x => bufferMbSize / (x / 1000))
    val writeSpeeds = writeResults.map(x => mbSize / (x / 1000))
    val readSpeeds = readResults.map(x => mbSize / (x / 1000))

    //threadPool.shutdown()
    client.session.close()
    client.cluster.close()

    //logger.info(statistics("Alloc", allocationSpeeds, bufferMbSize))
    logger.info(statistics(" Read", readSpeeds, mbSize))
    logger.info(statistics("Total", writeSpeeds, mbSize))

    writeResults.zip(readResults).zip(allocationResults).zip(bindResult).zip(executeResult).foreach { case ((((write, read), allocate), bind), execute) =>
      logger.info(f"read: ${read / 1000d}%2.2fs, bind+async ${allocate / 1000d}%2.2f, bind ${bind / 1000d}%2.2f, execute ${execute / 1000d}%2.2f total ${write / 1000d}%2.2f")
    }


  }

  def statistics(operation: String, list: List[Double], mbSize: Double): String = {
    val maxSpeed = list.max[Double]
    val minSpeed = list.min[Double]
    val avgSpeed = list.sum / writeResults.size
    val variSpeed = (list.map(x => x - avgSpeed).map(y => y * y).sum) / list.size
    val devTime = Math.sqrt(variSpeed)
    f"${operation}: ${list.size}, file size ${mbSize}%2.2f Mb, speed: min ${minSpeed}%2.2f Mb/s, max ${maxSpeed}%2.2f Mb/s, avg ${avgSpeed}%2.2f Mb/s, std dev ${devTime}%2.2f"
  }

  def clean(session: Session, tableName: String) = {
    session.execute( s"TRUNCATE ${tableName}")
  }

  def createSchema(session: Session, keyspace: String, tableName: String): Unit = {


    session.execute( s"""
                   CREATE KEYSPACE IF NOT EXISTS $keyspace
                   WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};"""
      .stripMargin
    )

    session.execute(
      s"""
        |USE  ${keyspace};
      """.stripMargin)

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS ${tableName} (
                                                   |uuid uuid,
                                                   |bucket bigint,
                                                   |start bigint,
                                                   |end bigint,
                                                   |data blob,
                                                   |PRIMARY KEY ((uuid, bucket), start)
                                                   |) WITH CLUSTERING ORDER BY (start ASC);
                                                   | """.stripMargin)

    clean(session, tableName)
  }


  def write(uuid: UUID, database: Database, bucketIndex: Int, bucketSize: Int, chunkSize: Int,
            blob: Array[Byte], statement: PreparedStatement, readSize: Int): Unit = {


    var count = 0
    var size = 0
    var len = 0
    var start = 0
    var time = 0l
    var time2 = 0l

    //Javish loop



    while (size < readSize) {

      len = Math.min(chunkSize, readSize - size)
      start = ((count % bucketSize)) * chunkSize

      val i = writeCounter / MyConfig.bucketSize


      time = System.currentTimeMillis()
      val boundStatement = statement.bind()
        .setUUID(0, uuid)
        .setLong(1, i)
        .setLong(2, start)
        .setLong(3, start + len)
        //.setBytes(4, ByteBuffer.allocate(0)))(s, executor)
        .setBytes(4, ByteBuffer.wrap(blob, count * chunkSize, len))
      bindTime += System.currentTimeMillis() - time

      writeCounter += 1
      size += len

      count += 1


      time2 = System.currentTimeMillis()
      val f = database.session.executeAsync(boundStatement)
      futures += f
      executeWithAsynchTime += System.currentTimeMillis() - time2
    }


  }


}

