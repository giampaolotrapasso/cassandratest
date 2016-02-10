package net.cassandratest

import java.io.{RandomAccessFile, IOException}
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.util.{UUID, ArrayList}

import com.datastax.driver.core.exceptions.WriteTimeoutException
import com.datastax.driver.core.policies.{TokenAwarePolicy, RoundRobinPolicy}
import com.datastax.driver.core.{PoolingOptions, Row, ProtocolVersion, PreparedStatement, ResultSet, ResultSetFuture, Session, BoundStatement, Cluster}
import com.google.common.util.concurrent.{FutureCallback, Futures}
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

class Database(node: String) extends LazyLogging {

  val poolingOptions = new PoolingOptions()


  var clusterBuilder = Cluster.builder.addContactPoint(node)
    .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
    .withPoolingOptions(poolingOptions)

  if (MyConfig.tokenAwareDriver)
    clusterBuilder = clusterBuilder.withLoadBalancingPolicy((new TokenAwarePolicy(new RoundRobinPolicy())))

  val cluster = clusterBuilder.build

  val session = cluster.newSession()
}

case class Record(uuid: UUID, bytes: Array[Byte], bucket: Int, start: Int, end: Int, offset: Int, length: Int)


object MyApp extends LazyLogging {

  val futures = new ArrayList[ResultSetFuture]()
  val client = new Database(MyConfig.serverName)


  def main(args: Array[String]): Unit = {
    createSchema(client.session)

    println("parameters "+ MyConfig.config.toString)

    val statement = client.session.prepare("INSERT INTO blobTest.store(uuid, bucket, start, end, data) VALUES (?, ?, ?, ?, ?);")

    var time = System.currentTimeMillis
    val byteArray: Array[Byte] = Files.readAllBytes(Paths.get(MyConfig.fileName))
    var elapsed = System.currentTimeMillis - time
    val size = byteArray.length
    val mbSize = size / 1000000.0
    logger.info(s"Time needed to read ${elapsed / 1000.0}s, ${size / 1000.0 / elapsed}, size ${mbSize} MB")

    time = System.currentTimeMillis
    write(client,
      bucketSize = MyConfig.bucketSize,
      chunkSize = MyConfig.chunkSize,
      maxConcurrentWrites = MyConfig.maxFutures,
      byteArray,
      statement)
    elapsed = System.currentTimeMillis - time

    logger.info(s"Time needed to write ${elapsed / 1000.0}s, ${size / 1000.0 / elapsed}Mb/s, size $mbSize MB")

    client.cluster.close()

  }

  def createSchema(session: Session): Unit = {
    Try(session.execute(
      """
        |DROP KEYSPACE blobTest;
      """.stripMargin))


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
      """CREATE TABLE store (
        |    uuid uuid,
        |    bucket bigint,
        |    start bigint,
        |    end bigint,
        |    data blob,
        |    PRIMARY KEY ((uuid, bucket), start)
        |) WITH CLUSTERING ORDER BY (start ASC);
        | """.stripMargin)
  }

  def waitDbWrites(): Unit = {
    while (futures.size() > 0) {
      // Wait for the other write requests to terminate
      val future = futures.get(0);
      val r = future.getUninterruptibly
      futures.remove(0)
    }
  }


  def write(database: Database, bucketSize: Int, chunkSize: Int, maxConcurrentWrites: Int,
            blob: Array[Byte], statement: PreparedStatement): Unit = {

    val uuid: UUID = UUID.randomUUID()
    var count = 0;

    var size = 0


    val numberOfRecords = (blob.size / chunkSize) +1

    //Javish loop
    while (size < blob.size) {
      val len = Math.min(chunkSize, blob.length-size)

      val record = Record(
        uuid = uuid,
        bucket = count / bucketSize,
        start = ((count % bucketSize)) * chunkSize,
        end = ((count % bucketSize) + 1) * chunkSize,
        bytes = blob,
        offset = count * chunkSize,
        length = len
      )

      asynchWrite(database, maxConcurrentWrites, statement, record)
      size += len
      count += 1
    }

    waitDbWrites()

  }


  def asynchWrite(database: Database, maxConcurrentWrites: Int, statement: PreparedStatement, record: Record): Unit = {
    val f: ResultSetFuture = database.session.executeAsync(statement.bind()
      .setUUID(0, record.uuid)
      .setLong(1, record.bucket)
      .setLong(2, record.start)
      .setLong(3, record.end)
      .setBytes(4, ByteBuffer.wrap(record.bytes, record.offset, record.length)))

    if (futures.size() > maxConcurrentWrites)
      waitDbWrites()

    futures.add(f)

    Futures.addCallback(f, new FutureCallback[ResultSet] {

      override def onFailure(t: Throwable): Unit = {
        val to = t.asInstanceOf[WriteTimeoutException]
        logger.error(s"Failed with: $t")
      }

      override def onSuccess(result: ResultSet): Unit = {}


    })
  }
}

