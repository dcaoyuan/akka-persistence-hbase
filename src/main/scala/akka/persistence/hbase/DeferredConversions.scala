package akka.persistence.hbase

import com.stumbleupon.async._
import scala.concurrent.{ ExecutionContext, Promise, Future }

trait DeferredConversions {

  implicit def typedFuture2unitFuture[T](f: Future[T])(implicit executionContext: ExecutionContext): Future[Unit] =
    f map { _ => () }

  implicit def deferred2unitFuture[T <: AnyRef](d: Deferred[AnyRef])(implicit executionContext: ExecutionContext): Future[Unit] =
    deferred2future(d)

  implicit def deferred2future[T <: AnyRef](d: Deferred[T]): Future[T] = {
    val p = Promise[T]()

    val onSuccess = new Callback[AnyRef, T] {
      def call(in: T) = p.success(in)
    }

    val onError = new Callback[Any, Exception] {
      def call(ex: Exception) = p.failure(ex)
    }

    d.addCallback(onSuccess).addErrback(onError)

    p.future
  }

  implicit def fun2callback[T <: AnyRef, R <: AnyRef](fn: T => R): Callback[R, T] =
    new Callback[R, T] {
      def call(arg: T): R = fn(arg)
    }
}

object DeferredConversions extends DeferredConversions