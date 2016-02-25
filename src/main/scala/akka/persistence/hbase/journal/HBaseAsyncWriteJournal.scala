package akka.persistence.hbase.journal

import akka.Done
import akka.actor.{ Actor, ActorLogging, ActorRef, Props, ExtendedActorSystem, NoSerializationVerificationNeeded }
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.persistence.hbase.HBaseClientFactory
import akka.persistence.hbase.RowKey
import akka.persistence.hbase.journal.Operator.AllOpsSubmitted
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.journal.Tagged
import akka.persistence.{ PersistentRepr, AtomicWrite }
import akka.serialization.Serialization
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import com.google.common.base.Stopwatch
import java.nio.ByteBuffer
import java.util.Locale
import org.apache.hadoop.hbase.client.{ HTable, Scan }
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._

import scala.collection.immutable
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object HBaseAsyncWriteJournal {
  private case class WriteFinished(pid: String, f: Future[Done]) extends NoSerializationVerificationNeeded
  private case class MessageId(persistenceId: String, sequenceNr: Long)
  private case class SerializedAtomicWrite(persistenceId: String, payload: Seq[Serialized])
  private case class Serialized(persistenceId: String, sequenceNr: Long, serialized: ByteBuffer, tags: Set[String],
                                eventManifest: String, serManifest: String, serId: Int, writerUuid: String)
}

/**
 * Asyncronous HBase Journal.
 *
 * Uses AsyncBase to implement asynchronous IPC with HBase.
 */
class HBaseAsyncWriteJournal extends AsyncWriteJournal with HBaseAsyncRecovery with HBaseJournalBase {

  import HBaseAsyncWriteJournal._
  import akka.persistence.hbase.TestingEventProtocol._
  import akka.persistence.hbase.journal.RowTypeMarkers._

  private lazy val config = context.system.settings.config

  implicit lazy val hBasePersistenceSettings = new HBaseJournalConfig(config)

  override def serialization = SerializationExtension(context.system)

  lazy val table = hBasePersistenceSettings.table
  lazy val family = hBasePersistenceSettings.family
  lazy val hadoopConfig = hBasePersistenceSettings.hadoopConfiguration

  lazy val client = HBaseClientFactory.getClient(hBasePersistenceSettings)

  lazy val hTable = new HTable(hadoopConfig, tableBytes)

  lazy val publishTestingEvents = hBasePersistenceSettings.publishTestingEvents

  implicit override val pluginDispatcher = context.system.dispatchers.lookup(hBasePersistenceSettings.pluginDispatcherId)

  import akka.persistence.hbase.Columns._
  import akka.persistence.hbase.DeferredConversions._
  import org.apache.hadoop.hbase.util.Bytes._

  val pubsubMinimumInterval: Duration = {
    import akka.util.Helpers.{ ConfigOps, Requiring }
    val key = "pubsub-minimum-interval"
    config.getString(key).toLowerCase(Locale.ROOT) match {
      case "off" ⇒ Duration.Undefined
      case _     ⇒ config.getMillisDuration(key) requiring (_ > Duration.Zero, key + " > 0s, or off")
    }
  }
  // Announce written changes to DistributedPubSub if pubsub-minimum-interval is defined in config
  private val pubsub = pubsubMinimumInterval match {
    case interval: FiniteDuration =>
      // PubSub will be ignored when clustering is unavailable
      Try {
        DistributedPubSub(context.system)
      }.toOption flatMap { extension =>
        if (extension.isTerminated)
          None
        else
          Some(context.actorOf(PubSubThrottler.props(extension.mediator, interval)
            .withDispatcher(context.props.dispatcher)))
      }

    case _ => None
  }
  // journal plugin api impl -------------------------------------------------------------------------------------------

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]) /*: Future[Seq[Try[Unit]]]*/ = {
    // we need to preserve the order / size of this sequence even though we don't map
    // AtomicWrites 1:1 with a C* insert
    //
    // We must NOT catch serialization exceptions here because rejections will cause
    // holes in the sequence number series and we use the sequence numbers to detect
    // missing (delayed) events in the eventByTag query.
    //
    // Note that we assume that all messages have the same persistenceId, which is
    // the case for Akka 2.4.2.

    def serialize(aw: AtomicWrite): SerializedAtomicWrite =
      SerializedAtomicWrite(
        aw.persistenceId,
        aw.payload.map { pr =>
          val (pr2, tags) = pr.payload match {
            case Tagged(payload, tags) =>
              (pr.withPayload(payload), tags)
            case _ => (pr, Set.empty[String])
          }
          serializeEvent(pr2, tags)
        })

    def publishTagNotification(serialized: Seq[SerializedAtomicWrite], result: Future[_]): Unit = {
      if (pubsub.isDefined) {
        result.foreach { _ =>
          for (
            p <- pubsub;
            tag: String <- serialized.map(_.payload.map(_.tags).flatten).flatten.toSet
          ) {
            p ! DistributedPubSubMediator.Publish("akka.persistence.hbase.journal.tag", tag)
          }
        }
      }
    }

    log.debug(s"Write async for {} presistent messages", messages.size)
    val watch = Stopwatch.createStarted()
    val maxMessageBatchSize = 100

    val p = Promise[Done]
    val pid = messages.head.persistenceId

    Future(messages.map(serialize)).flatMap { serialized =>
      val result =
        if (messages.size <= maxMessageBatchSize) {
          // optimize for the common case
          writeMessages(serialized)
        } else {
          val groups: List[Seq[SerializedAtomicWrite]] = serialized.grouped(maxMessageBatchSize).toList

          // execute the groups in sequence
          def rec(todo: List[Seq[SerializedAtomicWrite]], acc: List[Unit]): Future[List[Unit]] =
            todo match {
              case write :: remainder => writeMessages(write).flatMap(result => rec(remainder, result :: acc))
              case Nil                => Future.successful(acc.reverse)
            }
          rec(groups, Nil)
        }

      //log.debug("Putting into: {}", RowKey(selectPartition(sequenceNr), persistenceId, sequenceNr).toKeyString)
      result.onComplete { _ =>
        flushWrites()
        self ! WriteFinished(pid, p.future)
        p.success(Done)
      }(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)

      log.debug("Completed writing {} messages (took: {})", messages.size, watch.stop()) // todo better failure / success?
      if (publishTestingEvents) context.system.eventStream.publish(FinishedWrites(messages.size))

      publishTagNotification(serialized, result)
      // Nil == all good
      result.map(_ => List[Try[Unit]]())(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)
    }
  }

  private def writeMessages(atomicWrites: Seq[SerializedAtomicWrite]): Future[Unit] = {
    val persistenceId: String = atomicWrites.head.persistenceId
    val all = atomicWrites.flatMap(_.payload)
    val writes = all map { m =>
      val sequenceNr = m.sequenceNr
      executePut(
        RowKey(selectPartition(sequenceNr), persistenceId, sequenceNr).toBytes,
        Array(PersistenceId, SequenceNr, Marker, Message),
        Array(toBytes(persistenceId), toBytes(sequenceNr), toBytes(AcceptedMarker), m.serialized.array))
    }
    Future.sequence(writes).map(_ => ())
  }

  // todo should be optimised to do ranged deletes
  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val watch = Stopwatch.createStarted()
    log.debug(s"AsyncDeleteMessagesTo for persistenceId: {} to sequenceNr: {} (inclusive)", persistenceId, toSequenceNr)

    // prepare delete function (delete or mark as deleted)
    val permanent = true // TODO
    val doDelete = deleteFunctionFor(permanent)

    def scanAndDeletePartition(part: Long, operator: ActorRef): Unit = {
      val stopSequenceNr = if (toSequenceNr < Long.MaxValue) toSequenceNr + 1 else Long.MaxValue
      val startScanKey = RowKey.firstInPartition(persistenceId, part) // 021-ID-000000000000000000
      val stopScanKey = RowKey.lastInPartition(persistenceId, part, stopSequenceNr) // 021-ID-9223372036854775800
      val persistenceIdRowRegex = RowKey.patternForProcessor(persistenceId) //  .*-ID-.*

      // we can avoid canning some partitions - guaranteed to be empty for smaller than the partition number seqNrs
      if (part > toSequenceNr)
        return

      log.debug("Scanning for keys to delete, start: {}, stop: {}, regex: {}", startScanKey.toKeyString, stopScanKey.toKeyString, persistenceIdRowRegex)

      val scan = new Scan
      scan.setStartRow(startScanKey.toBytes)
      scan.setStopRow(stopScanKey.toBytes)
      scan.setBatch(hBasePersistenceSettings.scanBatchSize)

      val fl = new FilterList()
      fl.addFilter(new FirstKeyOnlyFilter)
      fl.addFilter(new KeyOnlyFilter)
      fl.addFilter(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(persistenceIdRowRegex)))
      scan.setFilter(fl)

      val scanner = hTable.getScanner(scan)
      try {
        var res = scanner.next()
        while (res != null) {
          operator ! res.getRow
          res = scanner.next()
        }
      } finally {
        scanner.close()
      }
    }

    val deleteRowsPromise = Promise[Unit]()
    val operator = context.actorOf(Operator.props(deleteRowsPromise, doDelete, hBasePersistenceSettings.pluginDispatcherId))

    val partitions = hBasePersistenceSettings.partitionCount
    val partitionScans = (1 to partitions).map(partitionNr => Future { scanAndDeletePartition(partitionNr, operator) })
    Future.sequence(partitionScans) onComplete { _ => operator ! AllOpsSubmitted }

    deleteRowsPromise.future map {
      case _ =>
        log.debug("Finished deleting messages for persistenceId: {}, to sequenceNr: {}, (took: {})", persistenceId, toSequenceNr, watch.stop())
        if (publishTestingEvents) context.system.eventStream.publish(FinishedDeletes(toSequenceNr))
    }
  }

  // end of journal plugin api impl ------------------------------------------------------------------------------------

  private def confirmAsync(persistenceId: String, sequenceNr: Long, channelId: String): Future[Unit] = {
    log.debug(s"Confirming async for persistenceId: {}, sequenceNr: {} and channelId: {}", persistenceId, sequenceNr, channelId)

    executePut(
      RowKey(sequenceNr, persistenceId, sequenceNr).toBytes,
      Array(Marker),
      Array(confirmedMarkerBytes(channelId)))
  }

  private def deleteFunctionFor(permanent: Boolean): (Array[Byte]) => Future[Unit] = {
    if (permanent) deleteRow
    else markRowAsDeleted
  }

  override def postStop(): Unit = {
    try hTable.close() finally client.shutdown()
    super.postStop()
  }

  private lazy val transportInformation: Option[Serialization.Information] = {
    val address = context.system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    if (address.hasLocalScope) None
    else Some(Serialization.Information(address, context.system))
  }

  private def serializeEvent(p: PersistentRepr, tags: Set[String]): Serialized = {
    def doSerializeEvent(): Serialized = {
      val event: AnyRef = p.payload.asInstanceOf[AnyRef]
      val serializer = serialization.findSerializerFor(event)
      val serManifest = serializer match {
        case ser2: SerializerWithStringManifest =>
          ser2.manifest(event)
        case _ =>
          if (serializer.includeManifest) event.getClass.getName
          else PersistentRepr.Undefined
      }
      val serEvent = ByteBuffer.wrap(serialization.serialize(event).get)
      Serialized(p.persistenceId, p.sequenceNr, serEvent, tags, p.manifest, serManifest,
        serializer.identifier, p.writerUuid)
    }

    // serialize actor references with full address information (defaultAddress)
    transportInformation match {
      case Some(ti) => Serialization.currentTransportInformation.withValue(ti) { doSerializeEvent() }
      case None     => doSerializeEvent()
    }
  }
}

/**
 * Actor which gets row keys and performs operations on them.
 * Completes the given `finish` promoise once all keys have been processed.
 *
 * Requires being notified when there's no more incoming work, by sending [[Operator.AllOpsSubmitted]]
 *
 * @param finish promise to complete one all ops have been applied to the submitted keys
 * @param op operation to be applied on each submitted key
 */
private[hbase] class Operator(finish: Promise[Unit], op: Array[Byte] => Future[Unit]) extends Actor with ActorLogging {

  var totalOps: Long = 0 // how many ops were we given to process (from user-land)
  var processedOps: Long = 0 // how many ops are pending to finish (from hbase-land)

  var allOpsSubmitted = false // are we don submitting ops to be applied?

  import akka.persistence.hbase.journal.Operator._
  import context.dispatcher

  def receive = {
    case key: Array[Byte] =>
      //      log.debug("Scheduling op on: {}", Bytes.toString(key))
      totalOps += 1
      op(key) foreach { _ => self ! OpApplied(key) }

    case AllOpsSubmitted =>
      log.debug("Received a total of {} ops to execute.", totalOps)
      allOpsSubmitted = true

    case OpApplied(key) =>
      processedOps += 1

      if (allOpsSubmitted && (processedOps == totalOps)) {
        log.debug("Finished processing all {} ops, shutting down operator.", totalOps)
        finish.success(())
        context stop self
      }
  }
}
object Operator {

  def props(deleteRowsPromise: Promise[Unit], doDelete: Array[Byte] => Future[Unit], dispatcher: String): Props =
    Props(classOf[Operator], deleteRowsPromise, doDelete).withDispatcher(dispatcher)

  final case class OpApplied(row: Array[Byte])
  case object AllOpsSubmitted
}