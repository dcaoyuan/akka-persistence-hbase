package akka.persistence.hbase.snapshot

import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.persistence.hbase.RowKey
import akka.persistence.hbase.Session
import akka.persistence.hbase.SnapshotRowKey
import akka.persistence.hbase.TestingEventProtocol.DeletedSnapshotsFor
import akka.persistence.hbase.journal._
import akka.persistence.serialization.Snapshot
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{ SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria, PersistentRepr }
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import com.stumbleupon.async.Callback
import java.nio.ByteBuffer
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.KeyValue
import org.hbase.async.Scanner
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.control.NonFatal

private[snapshot] object HBaseSnapshotStore {
  private case object Init

  private case class Serialized(serialized: ByteBuffer, serManifest: String, serId: Int)

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

  import akka.persistence.hbase.Columns._
  import akka.persistence.hbase.journal.RowTypeMarkers._

  override def preStart(): Unit = {
    // eager initialization, but not from constructor
    self ! HBaseSnapshotStore.Init
  }

  override def receivePluginInternal: Receive = {
    case HBaseSnapshotStore.Init =>
      try {
        HBaseJournalInit.createTable(context.system.settings.config, config.snapshotTable, config.snapshotFamily)
        //hbaseSession
      } catch {
        case NonFatal(e) =>
          log.warning(
            "Failed to connect to Cassandra and initialize. It will be retried on demand. Caused by: {}",
            e.getMessage)
      }
  }

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    log.debug("Loading async for persistenceId: [{}] on criteria: {}", persistenceId, criteria)

    val f = Future(scanPartition(persistenceId, criteria))
    f onFailure { case x => log.error(x, "Unable to read snapshot for persistenceId: {}, on criteria: {}", persistenceId, criteria) }
    f
  }

  private def scanPartition(persistenceId: String, criteria: SnapshotSelectionCriteria): Option[SelectedSnapshot] = {
    val startScanKey = SnapshotRowKey.lastForPersistenceId(persistenceId, toSequenceNr = criteria.maxSequenceNr)
    val stopScanKey = persistenceId

    val scan = session.preparePrefixScan(startScanKey.toBytes, Bytes.toBytes(stopScanKey), persistenceId, onlyRowKeys = false)
    scan.addColumn(family, MESSAGE)
    scan.addColumn(family, SNAPSHOT_DATA)
    scan.setReversed(true)
    scan.setMaxResultSize(1)
    val scanner = session.htable.getScanner(scan)

    try {
      var res = scanner.next()
      while (res != null) {
        val seqNr = RowKey.extractSeqNr(res.getRow)
        val snapshot = extractSnapshot(res)
        if (seqNr <= criteria.maxSequenceNr) {
          return Some(SelectedSnapshot(SnapshotMetadata(persistenceId, seqNr), snapshot.data)) // todo timestamp)
        }

        res = scanner.next()
      }

      None
    } finally {
      scanner.close()
    }
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    Future(serializeSnapshot(snapshot)).flatMap { ser =>
      session.executePut(
        SnapshotRowKey(metadata.persistenceId, metadata.sequenceNr).toBytes,
        Array(
          MARKER,
          PERSISTENCE_ID,
          SEQUENCE_NR,
          SER_ID,
          SER_MANIFEST,
          MESSAGE),
        Array(
          SnapshotMarkerBytes,
          Bytes.toBytes(metadata.persistenceId),
          Bytes.toBytes(metadata.sequenceNr),
          Bytes.toBytes(ser.serId),
          Bytes.toBytes(ser.serManifest),
          ser.serialized.array))
    }
  }

  def deleteAsync(meta: SnapshotMetadata): Future[Unit] = {
    log.debug("Deleting snapshot for meta: {}", meta)
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

  private lazy val transportInformation: Option[Serialization.Information] = {
    val address = context.system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    if (address.hasLocalScope) None
    else Some(Serialization.Information(address, context.system))
  }

  private def serializeSnapshot(payload: Any): Serialized = {
    def doSerializeSnapshot(): Serialized = {
      val snapshot: AnyRef = payload.asInstanceOf[AnyRef]
      val serializer = serialization.findSerializerFor(snapshot)
      val serManifest = serializer match {
        case ser2: SerializerWithStringManifest =>
          ser2.manifest(snapshot)
        case _ =>
          if (serializer.includeManifest) snapshot.getClass.getName
          else PersistentRepr.Undefined
      }
      val serPayload = ByteBuffer.wrap(serialization.serialize(snapshot).get)
      Serialized(serPayload, serManifest, serializer.identifier)
    }

    // serialize actor references with full address information (defaultAddress)
    transportInformation match {
      case Some(ti) => Serialization.currentTransportInformation.withValue(ti) { doSerializeSnapshot() }
      case None     => doSerializeSnapshot()
    }
  }

  private[this] def extractSnapshot(row: Result): Snapshot =
    session.getValue(row, MESSAGE) match {
      case null =>
        Snapshot(deserializeSnapshot(row, session))
      case bytes =>
        // for backwards compatibility
        serialization.deserialize(bytes, classOf[Snapshot]).get
    }

  private def deserializeSnapshot(row: Result, session: Session): Any = {
    serialization.deserialize(
      session.getValue(row, SNAPSHOT_DATA),
      Bytes.toInt(session.getValue(row, SER_ID)),
      Bytes.toString(session.getValue(row, SER_MANIFEST))).get
  }
}
