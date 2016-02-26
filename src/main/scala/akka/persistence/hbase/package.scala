package akka.persistence

import scala.concurrent._
import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

package object hbase {
  def retry[T](n: Int)(fn: => T): T = {
    retry(n, 0)(fn)
  }

  @annotation.tailrec
  def retry[T](n: Int, delay: Long)(fn: => T): T = {
    Try { fn } match {
      case Success(x) => x
      case _ if n > 1 => {
        Thread.sleep(delay)
        retry(n - 1, delay)(fn)
      }
      case Failure(e) => throw e
    }
  }
}
