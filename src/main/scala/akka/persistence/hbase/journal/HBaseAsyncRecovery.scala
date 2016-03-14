package akka.persistence.hbase.journal

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.persistence.PersistentRepr
import akka.persistence.hbase.RowKey
import akka.persistence.hbase.journal.Resequencer.AllPersistentsSubmitted
import akka.persistence.journal._
import java.io.IOException
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.KeyValue
import scala.collection.mutable
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration._

trait HBaseAsyncRecovery extends AsyncRecovery with ActorLogging {
  this: HBaseAsyncWriteJournal =>

  private lazy val replayDispatcherId = config.replayDispatcherId

  implicit val pluginDispatcher = context.system.dispatchers.lookup(replayDispatcherId)

  import HBaseAsyncWriteJournal.deserializeEvent
  import akka.persistence.hbase.Columns._

  // async recovery plugin impl

  override def asyncReplayMessages(
    persistenceId:  String,
    fromSequenceNr: Long,
    toSequenceNr:   Long,
    max:            Long
  )(replayCallback: PersistentRepr => Unit): Future[Unit] = max match {
    case 0 =>
      log.debug(
        "Skipping async replay for persistenceId [{}], from sequenceNr: [{}], to sequenceNr: [{}], since max messages count to replay is 0",
        persistenceId, fromSequenceNr, toSequenceNr
      )

      Future.successful(()) // no need to do a replay anything

    case _ =>
      log.debug(
        "Async replay for persistenceId [{}], from sequenceNr: [{}], to sequenceNr: [{}]{}",
        persistenceId, fromSequenceNr, toSequenceNr, if (max != Long.MaxValue) s", limited to: $max messages" else ""
      )

      val reachedSeqNrPromise = Promise[Long]()
      val loopedMaxFlag = new AtomicBoolean(false) // the resequencer may let us know that it looped `max` messages, and we can abort further scanning
      val resequencer = context.actorOf(Resequencer.props(persistenceId, fromSequenceNr, max, replayCallback, loopedMaxFlag, reachedSeqNrPromise, replayDispatcherId))

      val nPartitions = config.partitionCount
      val partitionScans = (1 to nPartitions).map(i => Future { scanPartition(i, persistenceId, fromSequenceNr, toSequenceNr, resequencer)(replayCallback) })

      Future.sequence(partitionScans) onSuccess {
        case lowestSeqNrInEachPartition =>
          val seqNrs = lowestSeqNrInEachPartition.filterNot(_ == 0L).toList

          if (seqNrs.nonEmpty)
            resequencer ! AllPersistentsSubmitted(seqNrs.min)
          else
            resequencer ! AllPersistentsSubmitted(0)
      }

      reachedSeqNrPromise.future map {
        case _ =>
          log.debug("Completed recovery scanning for persistenceId {}", persistenceId)
      }
  }

  private def scanPartition(part: Int, persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, resequencer: ActorRef)(replayCallback: PersistentRepr => Unit): Long = {
    val startScanKey = RowKey.firstInPartition(persistenceId, fromSequenceNr)(config) // 021-ID-0000000000000000021
    val stopSequenceNr = if (toSequenceNr < Long.MaxValue) toSequenceNr + 1 else Long.MaxValue
    val stopScanKey = RowKey.lastInPartition(persistenceId, stopSequenceNr) // 021-ID-9223372036854775800
    val persistenceIdRowRegex = RowKey.patternForProcessor(persistenceId) //  .*-ID-.*

    // we can avoid scanning some partitions - guaranteed to be empty for smaller than the partition number seqNrs
    if (part > toSequenceNr)
      return 0

    log.debug("Scanning {} partition for replay, from {} to {}", part, startScanKey.toKeyString, stopScanKey.toKeyString)

    val scan = session.preparePartitionScan(startScanKey, stopScanKey, persistenceIdRowRegex, onlyRowKeys = false)
    val scanner = session.htable.getScanner(scan)

    var lowestSeqNr: Long = 0L

    def resequenceMsg(persistentRepr: PersistentRepr) {
      val seqNr = persistentRepr.sequenceNr
      if (fromSequenceNr <= seqNr && seqNr <= toSequenceNr) {
        resequencer ! persistentRepr
        if (lowestSeqNr <= 0) {
          lowestSeqNr = seqNr // only set once
        }
      }
    }

    try {
      var row = scanner.next()
      while (row != null) {
        // Note: In case you wonder why we can't break the loop with a simple counter here once we loop through `max` elements:
        // Since this is multiple scans, on multiple partitions, they are not ordered, yet we must deliver ordered messages
        // to the receiver. Only the resequencer knows how many are really "delivered"

        val marker = session.getValue(row, MARKER)
        val event = extractEvent(row)
        extractMarker(row) match {
          case "A" =>
            resequenceMsg(event)

          case "S" =>
          // thanks to treating Snapshot rows as deleted entries, we won't suddenly apply a Snapshot() where the
          // our Processor expects a normal message. This is implemented for the HBase backed snapshot storage,
          // if you use the HDFS storage there won't be any snapshot entries in here.
          // As for message deletes: if we delete msgs up to seqNr 4, and snapshot was at 3, we want to delete it anyway.

          case "D" =>
            // mark as deleted, journal may choose to replay it
            resequenceMsg(event.update(deleted = true))

          case _ =>
            // channel confirmation

            //val channelId = RowTypeMarkers.extractSeqNrFromConfirmedMarker(marker)
            //replayCallback(event.update(confirms = channelId +: event.confirms))
            replayCallback(event)
        }
        row = scanner.next()
      }
      lowestSeqNr
    } catch {
      case ex: Throwable =>
        log.error("Scan replay msg failed: {}", ex.toString)
        lowestSeqNr
    } finally {
      log.debug("Done scheduling replays in partition {} (lowest seqNr: {})", part, lowestSeqNr)
      scanner.close()
    }
  }

  // todo make this multiple scans, on each partition instead of one big scan
  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug(s"Async read for highest sequence number for persistenceId: [$persistenceId] (hint, seek from  nr: [$fromSequenceNr])")

    val nPartitions = config.partitionCount
    val partitionScans = (1 to nPartitions).map(i => Future { scanPartitionForMaxSeqNr(i, persistenceId, fromSequenceNr) })
    Future.sequence(partitionScans) map {
      case seqNrs if seqNrs.isEmpty => 0L
      case seqNrs                   => seqNrs.max
    } map { seqNr =>
      log.debug("Found highest seqNr for persistenceId: {}, it's: {}", persistenceId, seqNr)
      seqNr
    }
  }

  private def scanPartitionForMaxSeqNr(part: Int, persistenceId: String, fromSequenceNr: Long): Long = {
    val startScanKey = RowKey.firstInPartition(persistenceId, fromSequenceNr) // 021-ID-0000000000000000021
    val stopScanKey = RowKey.lastInPartition(persistenceId, Long.MaxValue) // 021-ID-9223372036854775897
    val persistenceIdRowRegex = RowKey.patternForProcessor(persistenceId) //  .*-ID-.*

    //      log.debug("Scanning {} partition, from {} to {}", part, startScanKey.toKeyString, stopScanKey.toKeyString)

    val scan = session.preparePartitionScan(startScanKey, stopScanKey, persistenceIdRowRegex, onlyRowKeys = true)
    val scanner = session.htable.getScanner(scan)

    var highestSeqNr: Long = 0L
    try {
      var res = scanner.next()
      while (res != null) {
        val seqNr = RowKey.extractSeqNr(res.getRow)
        highestSeqNr = math.max(highestSeqNr, seqNr)
        res = scanner.next()
      }
      highestSeqNr
    } finally {
      if (highestSeqNr > 0) log.debug("Done scheduling replays in partition {} (highest seqNr: {})", part, highestSeqNr)
      scanner.close()
    }
  }
  //  end of async recovery plugin impl

  private def sequenceNr(columns: mutable.Buffer[KeyValue]): Long = {
    val messageKeyValue = session.findColumn(columns, MESSAGE)
    val msg = persistentFromBytes(messageKeyValue.value)
    msg.sequenceNr
  }

  private[this] def extractMarker(row: Result): String = {
    session.getValue(row, MARKER) match {
      case null => ""
      case b    => Bytes.toString(b)
    }
  }

  private[this] def extractEvent(row: Result): PersistentRepr =
    session.getValue(row, MESSAGE) match {
      case null =>
        PersistentRepr(
          payload = deserializeEvent(serialization, row, session),
          sequenceNr = Bytes.toLong(session.getValue(row, SEQUENCE_NR)),
          persistenceId = Bytes.toString(session.getValue(row, PERSISTENCE_ID)),
          manifest = Bytes.toString(session.getValue(row, EVENT_MANIFEST)),
          deleted = false,
          sender = null,
          writerUuid = Bytes.toString(session.getValue(row, WRITER_UUID))
        )
      case b =>
        // for backwards compatibility
        serialization.deserialize(b, classOf[PersistentRepr]).get
    }

  private[this] def persistentFromBytes(bytes: Array[Byte]): PersistentRepr =
    serialization.deserialize(bytes, classOf[PersistentRepr]).get

  private[this] def persistentFromByteBuffer(b: ByteBuffer): PersistentRepr =
    serialization.deserialize(Bytes.getBytes(b), classOf[PersistentRepr]).get

}

/**
 * This is required because of the way we store messages in the HTable (prefixed with a seed, in order to avoid the "hot-region problem").
 *
 * Note: The hot-region problem is when a lot of traffic goes to exactly one region, while the other regions "do nothing".
 *       This problem happens esp. with sequential numbering - such as the sequenceNr. The prefix-seeding solves this problem
 *       but it introduces out-of-sequence order scanning (a scan will read 000-a-05 before 001-a-01), which is wy the [[Resequencer]] is needed.
 *
 * @param replayCallback the callback which we want to call with sequenceNr ascending-order messages
 * @param maxMsgsToSequence max number of messages to be resequenced, usualy Long.MaxValue, but can be used to perform partial replays
 * @param loopedMaxFlag switched to `true` once `maxMsgsToSequence` is reached, with the goal of shortcircutting scanning the HTable
 * @param sequenceStartsAt since we support partial replays (from 4 to 100), the resequencer must know when to start replaying
 */
private[hbase] class Resequencer(
    persistenceId:     String,
    _sequenceStartsAt: Long,
    maxMsgsToSequence: Long,
    replayCallback:    PersistentRepr => Unit,
    loopedMaxFlag:     AtomicBoolean,
    reachedSeqNr:      Promise[Long]
) extends Actor with ActorLogging {

  implicit lazy val config = new HBaseJournalConfig(context.system.settings.config)

  private var allSubmitted = false

  private var sequenceStartsAt: Long = _sequenceStartsAt
  private var deliveredSeqNr = sequenceStartsAt - 1
  private def deliveredMsgs = deliveredSeqNr - sequenceStartsAt + 1
  val timeout = 1000.seconds // TODO configurable

  import Resequencer._
  import context.dispatcher

  private val delayed = mutable.Map.empty[Long, PersistentRepr]
  val timeoutTask = Some(context.system.scheduler.scheduleOnce(timeout, self, Timeout))

  def receive = {
    case p: PersistentRepr =>
      //      log.debug("Resequencing {} from {}; Delivered until {} already", p.payload, p.sequenceNr, deliveredSeqNr)
      resequence(p)

    case AllPersistentsSubmitted(assumeSequenceStartsAt) =>
      if (deliveredMsgs == 0L) {
        // kick off recovery from the assumed lowest seqNr
        // could be not 1 because of permanent deletion, yet replay was requested from 1
        sequenceStartsAt = assumeSequenceStartsAt
        deliveredSeqNr = sequenceStartsAt - 1

        val ro = delayed.remove(deliveredSeqNr + 1)
        if (ro.isDefined) resequence(ro.get)
      }

      if (delayed.isEmpty) {
        successResequencing()
      } else {
        allSubmitted = true
        failureResequencing(delayed)
      }

    case Timeout =>
      failureResequencing(delayed)
  }

  @scala.annotation.tailrec
  private def resequence(p: PersistentRepr) {
    if (p.sequenceNr == deliveredSeqNr + 1) {
      deliveredSeqNr = p.sequenceNr
      //      log.debug("Applying {} @ {}", p.payload, p.sequenceNr)
      replayCallback(p)

      if (deliveredMsgs == maxMsgsToSequence) {
        delayed.clear()
        loopedMaxFlag set true

        successResequencing()
      } else if (allSubmitted && delayed.isEmpty) {
        successResequencing()
      }
    } else {
      delayed += (p.sequenceNr -> p)
    }

    val ro = delayed.remove(deliveredSeqNr + 1)
    if (ro.isDefined) resequence(ro.get)
  }

  private def successResequencing() {
    log.debug("All messages have been resequenced and applied (until seqNr: {}, nr of messages: {})!", deliveredSeqNr, deliveredMsgs)
    timeoutTask foreach { _.cancel }
    reachedSeqNr success deliveredSeqNr
    context stop self
  }

  private def failureResequencing(delayed: collection.Map[Long, PersistentRepr]) {
    log.error(
      "All persistents submitted but delayed is not empty, some messages must fail to replay: {}",
      delayed.keys.map { sequenceNr =>
        RowKey(persistenceId, sequenceNr).toKeyString
      }.mkString(", ")
    )
    reachedSeqNr failure new IOException("Failed to complete resequence replay msgs.")
    context stop self
  }
}

private[hbase] object Resequencer {

  def props(persistenceId: String, sequenceStartsAt: Long, maxMsgsToSequence: Long, replayCallback: PersistentRepr => Unit, loopedMaxFlag: AtomicBoolean, reachedSeqNr: Promise[Long], dispatcherId: String) =
    Props(classOf[Resequencer], persistenceId, sequenceStartsAt, maxMsgsToSequence, replayCallback, loopedMaxFlag, reachedSeqNr).withDispatcher(dispatcherId) // todo stop it at some point

  case object Timeout
  final case class AllPersistentsSubmitted(assumeSequenceStartsAt: Long)
}