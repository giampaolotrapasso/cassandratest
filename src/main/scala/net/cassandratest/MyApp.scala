package net.cassandratest

import java.io.IOException
import java.nio.ByteBuffer
import java.util.{UUID, ArrayList}

import com.datastax.driver.core.exceptions.WriteTimeoutException
import com.datastax.driver.core.{PoolingOptions, Row, ProtocolVersion, PreparedStatement, ResultSet, ResultSetFuture, Session, BoundStatement, Cluster}
import com.google.common.util.concurrent.{FutureCallback, Futures}
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

class Database(node: String) extends LazyLogging {

  val poolingOptions = new PoolingOptions()

  val cluster = Cluster.builder.addContactPoint(node)
    .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
    .withPoolingOptions(poolingOptions)
    .build

  val session = cluster.newSession()
}

case class Record(uuid: UUID, bytes: Array[Byte], bucket: Int, start: Int, end: Int)


object MyApp extends LazyLogging {

  val futures = new ArrayList[ResultSetFuture]()
  val client = new Database(MyConfig.serverName)


  def main(args: Array[String]): Unit = {
    createSchema(client.session)

    val statement = client.session.prepare("INSERT INTO blobTest.store(uuid, bucket, start, end, data) VALUES (?, ?, ?, ?, ?);")

    val blob = new Array[Byte](MyConfig.blobSize)
    scala.util.Random.nextBytes(blob)

    val time = System.currentTimeMillis()
    write(client,
      numberOfRecords = MyConfig.recordNumber,
      bucketSize = MyConfig.bucketSize,
      maxConcurrentWrites = MyConfig.maxFutures,
      blob,
      statement)

    val elapsed = (System.currentTimeMillis() - time)
    val size = MyConfig.recordNumber*MyConfig.blobSize
    logger.info(s"Time needed ${elapsed / 1000.0}s, ${size/1000.0 / elapsed}, size $size MB")

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


  def write(database: Database, numberOfRecords: Int, bucketSize: Int, maxConcurrentWrites: Int,
            blob: Array[Byte], statement: PreparedStatement): Unit = {

    val uuid: UUID = UUID.randomUUID()
    var count = 0;

    //Javish loop
    while (count < numberOfRecords) {
      val record = Record(
        uuid = uuid,
        bucket = count / bucketSize,
        start = ((count % bucketSize)) * blob.length,
        end = ((count % bucketSize) + 1) * blob.length,
        bytes = blob
      )

      asynchWrite(database, maxConcurrentWrites, statement, record)

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
      .setBytes(4, ByteBuffer.wrap(record.bytes)))

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

