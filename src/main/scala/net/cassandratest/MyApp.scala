package net.cassandratest

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.Executors

import com.datastax.driver.core.{Cluster, HostDistance, PoolingOptions, PreparedStatement, ProtocolVersion, ResultSet, ResultSetFuture, Session}
import com.google.common.util.concurrent.{MoreExecutors, ListeningExecutorService}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.concurrent.{Future => SFuture, ExecutionContext, Await}
import scala.util.Try

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

case class Record(uuid: UUID, bytes: Array[Byte], start: Int, end: Int, offset: Int, length: Int)

case class FileInfo(size: Long, chunks: Int)

object MyApp extends LazyLogging with FutureHelper {

  var futures = List[SFuture[ResultSet]]()
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

  val threadPool = Executors.newFixedThreadPool(MyConfig.poolsize)

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(threadPool)

  implicit val executor: ListeningExecutorService = MoreExecutors.listeningDecorator(threadPool)


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

    val statement = client.session.prepare(s"INSERT INTO $keyspace.$tableName(uuid, start, end, data) VALUES (?, ?, ?, ?);")
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
          write(client,
            chunkSize = MyConfig.chunkSize,
            byteBuffer.array,
            statement, readSize)
          allocationTime += System.currentTimeMillis() - timeAllocationStarted
        }
      }
      finally {
        raf.close
      }
      val list = SFuture.sequence(futures)
      Await.ready(list, 60 seconds)


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

    threadPool.shutdown()
    client.session.close()
    client.cluster.close()

    //logger.info(statistics("Alloc", allocationSpeeds, bufferMbSize))
    logger.info(statistics(" Read", readSpeeds, mbSize))
    logger.info(statistics("Total", writeSpeeds, mbSize))

    writeResults.zip(readResults).zip(allocationResults).zip(bindResult).zip(executeResult).foreach { case ((((write, read), allocate), bind), execute) =>
      logger.info(f"read: ${read / 1000d}%2.2fs, bind+async ${allocate / 1000d}%2.2f, bind ${bind / 1000d}%2.2f, execute ${execute / 1000d}%2.2f total ${write / 1000d}%2.2f")
    }
  }

  def createSchema(session: Session): Unit = {
    Try(session.execute(
      """
        |DROP KEYSPACE blobTest;
      """.stripMargin))

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
    session.execute(s"TRUNCATE ${tableName}")
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
                                                   |start bigint,
                                                   |end bigint,
                                                   |data blob,
                                                   |PRIMARY KEY (uuid)
                                                   |);
                                                   | """.stripMargin)
  }


  def write(database: Database, chunkSize: Int,
            blob: Array[Byte], statement: PreparedStatement, readSize: Long): Unit = {

    val uuid: UUID = UUID.randomUUID()
    var count = 0;

    var size = 0


    val numberOfRecords = (blob.size / chunkSize) + 1

    //Javish loop
    while (size < blob.size) {
      val len = Math.min(chunkSize, blob.length - size)

      val record = Record(
        uuid = uuid,
        start = size,
        end = size + len,
        bytes = blob,
        offset = count * chunkSize,
        length = len
      )

      asynchWrite(database, 1, statement, record)
      size += len
      count += 1
    }


  }


  def asynchWrite(database: Database, maxConcurrentWrites: Int, statement: PreparedStatement, record: Record): Unit = {

    val f = executeWithAsynch(statement.bind()
      .setUUID(0, record.uuid)
      .setLong(1, record.start)
      .setLong(2, record.end)
      .setBytes(3, ByteBuffer.wrap(record.bytes, record.offset, record.length)))

    futures = futures :+ f

  }
}



