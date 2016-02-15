package net.cassandratest

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import com.datastax.driver.core.policies.{RoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.core.{HostDistance, Cluster, PoolingOptions, PreparedStatement, ProtocolVersion, ResultSet, Session}
import com.google.common.util.concurrent.{ListeningExecutorService, MoreExecutors}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future => SFuture}
import scala.util.{Success, Failure}

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

  var futures = List[SFuture[ResultSet]]()
  val client = new Database(MyConfig.serverName)
  var writeResults = List[Double]()
  var readResults = List[Double]()

  implicit val s = client.session

  var time = System.currentTimeMillis()

  val threadPool = Executors.newCachedThreadPool()

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

    val tableName = "store"
    createSchema(client.session, tableName)

    val statement = client.session.prepare("INSERT INTO blobTest.store(uuid, bucket, start, end, data) VALUES (?, ?, ?, ?, ?);")
    val fileInfo = getFileInfo(MyConfig.fileName)

    var fileSize = 0

    var i = 0;

    while (i < MyConfig.cycles) {
      clean(client.session, tableName)
      time = System.currentTimeMillis
      val raf = new RandomAccessFile(MyConfig.fileName, "r")
      val uuid: UUID = UUID.randomUUID()
      var readTime = 0d
      try {
        val byteBuffer = new Array[Byte](MyConfig.readbufferSize)

        for (chunkIndex <- 1 to fileInfo.chunks) {

          val seek = (chunkIndex - 1) * MyConfig.readbufferSize
          val timeStartRead = System.currentTimeMillis
          raf.seek(seek)
          val readSize = raf.read(byteBuffer)
          readTime += (System.currentTimeMillis - timeStartRead).toDouble

          write(uuid, client,
            bucketIndex = chunkIndex,
            bucketSize = MyConfig.bucketSize,
            chunkSize = MyConfig.chunkSize,
            byteBuffer.array,
            statement, readSize)
        }
      }
      finally {
        raf.close
      }

      val list = SFuture.sequence(futures)
      Await.ready(list, 60 seconds)
      val writeTime = (System.currentTimeMillis - time).toDouble
      writeResults = writeResults :+ writeTime
      readResults = readResults :+ readTime

      i += 1

    }
    val mbSize = fileInfo.size / 1000000.0

    val writeSpeeds = writeResults.map(x => mbSize / (x / 1000))
    val readSpeeds = readResults.map(x => mbSize / (x / 1000))

    threadPool.shutdown()
    client.session.close()
    client.cluster.close()

    logger.info(statistics(" Read", readSpeeds, mbSize))
    logger.info(statistics("Write", writeSpeeds, mbSize))

    writeResults.zip(readResults).foreach{ case (write,read) =>
      logger.info(f"read: ${read/1000d}%2.2fs, write ${write/1000d}%2.2f")
    }


  }

  def statistics(operation: String, list: List[Double], mbSize: Double) : String = {
    val maxSpeed = list.max[Double]
    val minSpeed = list.min[Double]
    val avgSpeed = list.sum / writeResults.size
    val variSpeed = (list.map(x => x - avgSpeed).map(y => y * y).sum) / list.size
    val devTime = Math.sqrt(variSpeed)
    f"${operation}: ${list.size}, file size ${mbSize}%2.2f Mb, speed: min ${minSpeed}%2.2f Mb/s, max ${maxSpeed}%2.2f Mb/s, avg ${avgSpeed}%2.2f Mb/s, std dev ${devTime}%2.2f"
  }

  def clean(session: Session, tableName: String) = {
    session.execute( s"""TRUNCATE ${tableName}""")
  }

  def createSchema(session: Session, tableName: String): Unit = {


    session.execute( """
                   CREATE KEYSPACE IF NOT EXISTS blobTest
                   WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};"""
      .stripMargin
    )

    session.execute(
      """
        |USE  blobTest;
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
            blob: Array[Byte], statement: PreparedStatement, readSize : Int): Unit = {


    var count = 0;

    var size = 0


    val numberOfRecords = (blob.size / chunkSize) + 1

    //Javish loop
    while (size < readSize) {
      val len = Math.min(chunkSize, readSize - size)
      val start = ((count % bucketSize)) * chunkSize

      val record = Record(
        uuid = uuid,
        bucket = bucketIndex,
        start = start,
        end = start + len,
        bytes = blob,
        offset = count * chunkSize,
        length = len
      )

      asynchWrite(database, statement, record)
      size += len
      count += 1
    }


  }


  def asynchWrite(database: Database, statement: PreparedStatement, record: Record): Unit = {
    val f = executeWithAsynch(statement.bind()
      .setUUID(0, record.uuid)
      .setLong(1, record.bucket)
      .setLong(2, record.start)
      .setLong(3, record.end)
      //.setBytes(4, ByteBuffer.allocate(0)))(s, executor)
      .setBytes(4, ByteBuffer.wrap(record.bytes, record.offset, record.length)))

    f.onComplete {
      case Success(value) => {} //println(s"Got the callback, meaning = $value")
      case Failure(e) => e.printStackTrace
    }



    futures = futures :+ f
  }
}

