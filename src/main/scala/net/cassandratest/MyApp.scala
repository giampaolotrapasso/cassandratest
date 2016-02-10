package net.cassandratest

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import com.datastax.driver.core.policies.{RoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.core.{Cluster, PoolingOptions, PreparedStatement, ProtocolVersion, ResultSet, Session}
import com.google.common.util.concurrent.{ListeningExecutorService, MoreExecutors}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future => SFuture}
import scala.util.{Success, Failure}


class Database(node: String) extends LazyLogging {

  val poolingOptions = new PoolingOptions()


  var clusterBuilder = Cluster.builder.addContactPoint(node)
    .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
    .withPoolingOptions(poolingOptions)

  val cluster = clusterBuilder.build

  val session = cluster.newSession()
}

case class Record(uuid: UUID, bytes: Array[Byte], bucket: Int, start: Int, end: Int, offset: Int, length: Int)


object MyApp extends LazyLogging with FutureHelper{

  var futures = List[SFuture[ResultSet]]()
  val client = new Database(MyConfig.serverName)

  implicit val s = client.session

  var time = System.currentTimeMillis()

  val threadPool = Executors.newCachedThreadPool()

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(threadPool)

  implicit val executor: ListeningExecutorService = MoreExecutors.listeningDecorator(threadPool)



  def main(args: Array[String]): Unit = {

    val tableName = "store"
    println("Table name "+ tableName)
    createSchema(client.session, tableName)

    val path = Paths.get(MyConfig.fileName)
    val buffer = ByteBuffer.allocate(MyConfig.readbufferSize)
    var position = 0

    //println("parameters " + MyConfig.config.toString)



    val statement = client.session.prepare("INSERT INTO blobTest.store(uuid, bucket, start, end, data) VALUES (?, ?, ?, ?, ?);")
    val b: Array[Byte] = Files.readAllBytes(Paths.get(MyConfig.fileName))
    val fileSize = b.length
    val mbSize = fileSize / 1000000

    time = System.currentTimeMillis

    write(client,
            bucketSize = MyConfig.bucketSize,
            chunkSize = MyConfig.chunkSize,
            b,
            statement)

    val list = SFuture.sequence(futures)
    Await.ready(list, 60 seconds)
    val elapsed = System.currentTimeMillis - time
    logger.info(s"Time needed  ${elapsed / 1000.0}s, ${fileSize / 1000.0 / elapsed}Mb/s, size $mbSize MB")

    threadPool.shutdown()
    client.cluster.close()

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
        |    uuid uuid,
        |    bucket bigint,
        |    start bigint,
        |    end bigint,
        |    data blob,
        |    PRIMARY KEY ((uuid, bucket), start)
        |) WITH CLUSTERING ORDER BY (start ASC);
        | """.stripMargin)

    session.execute(s"""TRUNCATE ${tableName}""")
  }



  def write(database: Database, bucketSize: Int, chunkSize: Int,
            blob: Array[Byte], statement: PreparedStatement): Unit = {

    val uuid: UUID = UUID.randomUUID()
    var count = 0;

    var size = 0


    val numberOfRecords = (blob.size / chunkSize) + 1

    //Javish loop
    while (size < blob.size) {
      val len = Math.min(chunkSize, blob.length - size)

      val record = Record(
        uuid = uuid,
        bucket = count / bucketSize,
        start = ((count % bucketSize)) * chunkSize,
        end = ((count % bucketSize) + 1) * chunkSize,
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

