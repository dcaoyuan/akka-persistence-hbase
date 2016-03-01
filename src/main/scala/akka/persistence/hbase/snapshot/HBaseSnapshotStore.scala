package akka.persistence.hbase.snapshot

import akka.Done
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.NoSerializationVerificationNeeded
import akka.persistence.hbase.RowKey
import akka.persistence.hbase.Session
import akka.persistence.hbase.SnapshotRowKey
import akka.persistence.hbase.TestingEventProtocol.DeletedSnapshotsFor
import akka.persistence.hbase.journal._
import akka.persistence.serialization.Snapshot
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria, PersistentRepr }
import akka.serialization.SerializationExtension
import com.stumbleupon.async.Callback
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.KeyValue
import org.hbase.async.Scanner
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

object HBaseSnapshotStore {
  private case object Init

  private case class WriteFinished(pid: String, f: Future[Done]) extends NoSerializationVerificationNeeded

  final class RecursiveScannerResultHandler(scanner: Scanner, promise: Promise[Seq[java.util.ArrayList[KeyValue]]]) extends Callback[AnyRef, java.util.ArrayList[java.util.ArrayList[KeyValue]]] {

    val results = mutable.ArrayBuffer[java.util.ArrayList[KeyValue]]()

    def call(rawRows: java.util.ArrayList[java.util.ArrayList[KeyValue]]) = {
      try {
        if (rawRows == null) { // null, reached the end of scan
          promise.success(results.toSeq)
        } else {
          val rowsIterator = rawRows.iterator()
          while (rowsIterator.hasNext) {
            val row = rowsIterator.next()
            results += row
          }

          scanner.nextRows().addCallback(this)
        }
      } catch {
        case e: Throwable => promise.failure(e)
      } finally {
        try {
          scanner.close()
        } finally {
          // Nothing can be done now
        }
      }
    }
  }
}
class HBaseSnapshotStore(val system: ActorSystem, val config: HBaseSnapshotConfig) extends SnapshotStore with ActorLogging {
  import HBaseSnapshotStore._

  val serialization = SerializationExtension(context.system)

  implicit val pluginDispatcher = system.dispatchers.lookup("akka-hbase-persistence-dispatcher")

  val table = Bytes.toBytes(config.snapshotTable)
  val family = Bytes.toBytes(config.snapshotFamily)
  val session = new Session(table, family, config)

  /** Snapshots we're in progress of saving */
  private val writeInProgress = new java.util.HashMap[String, Future[Done]]()

  import akka.persistence.hbase.Columns._
  import akka.persistence.hbase.journal.RowTypeMarkers._

  override def preStart(): Unit = {
    // eager initialization, but not from constructor
    self ! HBaseSnapshotStore.Init
  }

  override def receivePluginInternal: Receive = {
    case WriteFinished(persistenceId, f) =>
      writeInProgress.remove(persistenceId, f)
    case HBaseSnapshotStore.Init =>
      try {
        HBaseJournalInit.createTable(context.system.settings.config, config.snapshotTable, config.snapshotFamily)
        //hbaseSession
      } catch {
        case NonFatal(e) =>
          log.warning(
            "Failed to connect to Cassandra and initialize. It will be retried on demand. Caused by: {}",
            e.getMessage
          )
      }
  }

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    log.debug("Loading async for persistenceId: [{}] on criteria: {}", persistenceId, criteria)

    def scanPartition(): Option[SelectedSnapshot] = {
      val startScanKey = SnapshotRowKey.lastForPersistenceId(persistenceId, toSequenceNr = criteria.maxSequenceNr)
      val stopScanKey = persistenceId

      val scan = session.preparePrefixScan(startScanKey.toBytes, Bytes.toBytes(stopScanKey), persistenceId, onlyRowKeys = false)
      scan.addColumn(family, Message)
      scan.setReversed(true)
      scan.setMaxResultSize(1)
      val scanner = session.htable.getScanner(scan)

      try {
        var res = scanner.next()
        while (res != null) {
          val seqNr = RowKey.extractSeqNr(res.getRow)
          val messageCell = res.getColumnLatestCell(family, Message)

          val snapshot = snapshotFromBytes(CellUtil.cloneValue(messageCell))

          if (seqNr <= criteria.maxSequenceNr)
            return Some(SelectedSnapshot(SnapshotMetadata(persistenceId, seqNr), snapshot.data)) // todo timestamp)

          res = scanner.next()
        }

        None
      } finally {
        scanner.close()
      }
    }

    val f = Future(scanPartition())
    f onFailure { case x => log.error(x, "Unable to read snapshot for persistenceId: {}, on criteria: {}", persistenceId, criteria) }
    f
  }

  def saveAsync(meta: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    log.debug("Saving async, of {}", meta)

    val p = Promise[Done]
    val pid = meta.persistenceId
    writeInProgress.put(pid, p.future)

    val f = serialize(Snapshot(snapshot)) match {
      case Success(serializedSnapshot) =>
        session.executePut(
          SnapshotRowKey(meta.persistenceId, meta.sequenceNr).toBytes,
          Array(Marker, Message),
          Array(SnapshotMarkerBytes, serializedSnapshot)
        )

      case Failure(ex) =>
        Future failed ex
    }
    f.onComplete { _ =>
      log.debug("Saved: {}", meta)
      self ! WriteFinished(pid, p.future)
      p.success(Done)
    }(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)

    f
  }

  def deleteAsync(meta: SnapshotMetadata): Future[Unit] = {
    log.debug("Deleting snapshot for meta: {}", meta)

    val pid = meta.persistenceId
    writeInProgress.remove(pid)
    session.executeDelete(SnapshotRowKey(meta.persistenceId, meta.sequenceNr).toBytes)
  }

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    log.debug("Deleting snapshot for persistenceId: [{}], criteria: {}", persistenceId, criteria)
    val start = System.currentTimeMillis

    val toSeqNr = if (criteria.maxSequenceNr < Long.MaxValue) criteria.maxSequenceNr + 1 else Long.MaxValue
    val startKey = SnapshotRowKey.firstForPersistenceId(persistenceId)
    val stopKey = SnapshotRowKey.lastForPersistenceId(persistenceId, toSeqNr)

    val scanner = session.newScanner()
    scanner.setStartKey(startKey.toBytes)
    scanner.setStopKey(stopKey.toBytes)
    scanner.setKeyRegexp(s"""$persistenceId-.*""")

    val p = Promise[Seq[java.util.ArrayList[KeyValue]]]()

    try {
      val handler = new RecursiveScannerResultHandler(scanner, p)
      scanner.nextRows().addCallback(handler)
    } catch {
      case e: Throwable => p.failure(e)
    }

    p.future map { rows =>
      for {
        row <- rows if session.isSnapshotRow(row.asScala)
        col <- row.asScala.headOption
      } yield session.deleteRow(col.key)
    } map { _ =>
      session.flushWrites()
      log.debug("Finished deleting snapshots for persistenceId: {}, to sequenceNr: {}, (took: {})", persistenceId, toSeqNr, System.currentTimeMillis - start)
      if (config.publishTestingEvents) system.eventStream.publish(DeletedSnapshotsFor(persistenceId, criteria))
    }

  }

  // TODO

  protected def snapshotFromBytes(bytes: Array[Byte]): Snapshot =
    serialization.deserialize(bytes, classOf[Snapshot]).get

  protected def snapshotToBytes(msg: Snapshot): Array[Byte] =
    serialization.serialize(msg).get

  protected def persistentFromBytes(bytes: Array[Byte]): PersistentRepr =
    serialization.deserialize(bytes, classOf[PersistentRepr]).get

  //protected def persistentToBytes(msg: Persistent): Array[Byte] =
  // serialization.serialize(msg).get

  protected def deserialize(bytes: Array[Byte]): Try[Snapshot] =
    serialization.deserialize(bytes, classOf[Snapshot])

  protected def serialize(snapshot: Snapshot): Try[Array[Byte]] =
    serialization.serialize(snapshot)
}