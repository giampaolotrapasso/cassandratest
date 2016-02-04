package it.giampaolotrapasso

import java.io.IOException
import java.nio.ByteBuffer
import java.util.{UUID, ArrayList}

import com.datastax.driver.core.{PreparedStatement, ResultSet, ResultSetFuture, Session, BoundStatement, Cluster}
import com.google.common.util.concurrent.{FutureCallback, Futures}
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try


class Database(node: String) extends LazyLogging {

  val cluster = Cluster.builder.addContactPoint(node).build
  val session = cluster.newSession()
  logger.info(cluster.getMetadata.toString)


}

case class Record(uuid: UUID, bytes: Array[Byte], bucket: Int, start: Int, end: Int)


object MyApp extends LazyLogging {

  val futures = new ArrayList[ResultSetFuture]()


  def main(args: Array[String]): Unit = {
    val client = new Database("localhost")

    createSchema(client.session)


    val statement = client.session.prepare("INSERT INTO driverTest.mytable(uuid, bucket, start, end, data) VALUES (?, ?, ?, ?, ?) if not exists;")
    val boundStatement = new BoundStatement(statement)

    write(client, numberOfRecords = 100, bucketSize = 4, maxConcurrentWrites = 6, blobSize = 1000, boundStatement, async = true)
    client.cluster.close()

  }

  def createSchema(session: Session): Unit = {
    Try(session.execute(
      """
        |DROP KEYSPACE driverTest;
      """.stripMargin))


    session.execute( """
                   CREATE KEYSPACE IF NOT EXISTS driverTest
                   WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};"""
      .stripMargin
    )

    session.execute(
      """
        |USE  driverTest;
      """.stripMargin)

    session.execute(
      """CREATE TABLE mytable (
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
    logger.info("waiting for the db")
    while (futures.size() > 0) {
      // Wait for the other write requests to terminate
      val future = futures.get(0);
      val r = future.getUninterruptibly
      logger.info(s"Result $r")
      futures.remove(0);

    }
  }


  def write(database: Database, numberOfRecords: Int, bucketSize: Int, maxConcurrentWrites: Int,
            blobSize: Int, statement: BoundStatement, async: Boolean): Unit = {

    val uuid: UUID = UUID.randomUUID()
    var count = 0;

    //Javish loop
    while (count < numberOfRecords) {

      val bytes = new Array[Byte](blobSize)
      scala.util.Random.nextBytes(bytes)

      val record = Record(
        uuid = uuid,
        bucket = count / bucketSize,
        start = ((count % bucketSize)) * blobSize,
        end = ((count % bucketSize) + 1) * blobSize,
        bytes = bytes
      )

      logger.info(s"binding record $record")
      if (async)
        asynchWrite(database, maxConcurrentWrites, statement, record)
      else
        synchWrite(database, statement, record)
      count += 1
    }

    if (async)
      waitDbWrites()

  }

  def synchWrite(database: Database, statement: BoundStatement, record: Record): Unit = {
    val r = database.session.execute(statement.bind()
      .setUUID(0, record.uuid)
      .setLong(1, record.bucket)
      .setLong(2, record.start)
      .setLong(3, record.end)
      .setBytes(4, ByteBuffer.wrap(record.bytes)))

    logger.info(s"Applied ${r.wasApplied()}")
    logger.info(s" ${r.getExecutionInfo}")
  }


  def asynchWrite(database: Database, maxConcurrentWrites: Int, statement: BoundStatement, record: Record): Unit = {
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

      override def onFailure(t: Throwable): Unit = logger.error(s"Failed with: $t")

      override def onSuccess(result: ResultSet): Unit = logger.info(s"result $result")

    })
  }
}

