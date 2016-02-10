package net.cassandratest


import com.datastax.driver.core.{ ResultSet, Session, Statement }
import com.google.common.util.concurrent.{ListeningExecutorService, FutureCallback, Futures}

import scala.concurrent.{ Promise, Future }

trait FutureHelper {

  def executeWithAsynch(st: Statement)(implicit session: Session, executor: ListeningExecutorService): Future[ResultSet] = {
    execute(st).future
  }

  private def execute(st: Statement)(implicit session: Session, executor: ListeningExecutorService): Promise[ResultSet] = {
    val promise = Promise[ResultSet]()
    val future = session.executeAsync(st)

    val callback = new FutureCallback[ResultSet] {
      def onSuccess(result: ResultSet): Unit = {
        promise success result
      }

      def onFailure(err: Throwable): Unit = {
        promise failure err
      }
    }

    Futures.addCallback(future, callback, executor)
    promise
  }
}