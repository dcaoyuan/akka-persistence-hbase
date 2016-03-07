package akka.persistence.hbase.journal

import akka.Done
import akka.actor.{ Actor, ActorLogging, ActorRef, Props, ExtendedActorSystem, NoSerializationVerificationNeeded }
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.persistence.hbase.Session
import akka.persistence.hbase._
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
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.immutable
import scala.concurrent._
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.control.NonFatal

private[hbase] object HBaseAsyncWriteJournal {
  private case object Init

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
class HBaseAsyncWriteJournal extends AsyncWriteJournal with HBaseAsyncRecovery {

  import HBaseAsyncWriteJournal._
  import akka.persistence.hbase.TestingEventProtocol._
  import akka.persistence.hbase.journal.RowTypeMarkers._

  implicit val config = new HBaseJournalConfig(context.system.settings.config)

  val serialization = SerializationExtension(context.system)

  lazy val publishTestingEvents = config.publishTestingEvents

  implicit override val pluginDispatcher = context.system.dispatchers.lookup(config.pluginDispatcherId)

  import akka.persistence.hbase.Columns._
  import akka.persistence.hbase.DeferredConversions._
  import org.apache.hadoop.hbase.util.Bytes._

  // readHighestSequence must be performed after pending write for a persistenceId
  // when the persistent actor is restarted.
  // It seems like C* doesn't support session consistency so we handle it ourselves.
  // https://aphyr.com/posts/299-the-trouble-with-timestamps
  // But what about HBase
  private val writeInProgress = new java.util.HashMap[String, Future[Done]]()

  // Announce written changes to DistributedPubSub if pubsub-minimum-interval is defined in config
  private val pubsub = config.pubsubMinimumInterval match {
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

  private[journal] class HBaseSession(val underlying: Session) {

    //    executeCreateKeyspaceAndTables(underlying, config.keyspaceAutoCreate, maxTagId)
    //
    //    val preparedWriteMessage = underlying.prepare(writeMessage)
    //    val preparedDeleteMessages = underlying.prepare(deleteMessages)
    //    val preparedSelectMessages = underlying.prepare(selectMessages).setConsistencyLevel(readConsistency)
    //    val preparedCheckInUse = underlying.prepare(selectInUse).setConsistencyLevel(readConsistency)
    //    val preparedWriteInUse = underlying.prepare(writeInUse)
    //    val preparedSelectHighestSequenceNr = underlying.prepare(selectHighestSequenceNr).setConsistencyLevel(readConsistency)
    //    val preparedSelectDeletedTo = underlying.prepare(selectDeletedTo).setConsistencyLevel(readConsistency)
    //    val preparedInsertDeletedTo = underlying.prepare(insertDeletedTo).setConsistencyLevel(writeConsistency)
    //
    //    def protocolVersion: ProtocolVersion =
    //      underlying.getCluster.getConfiguration.getProtocolOptions.getProtocolVersion
  }

  private var sessionUsed = false

  private[journal] lazy val hbaseSession: HBaseSession = {
    retry(config.connectionRetries + 1, config.connectionRetryDelay.toMillis) {
      val table = Bytes.toBytes(config.table)
      val family = Bytes.toBytes(config.family)
      val underlying: Session = new Session(table, family, config)
      try {
        val s = new HBaseSession(underlying)
        //new CassandraConfigChecker {
        //  override def session: Session = s.underlying
        //  override def config: CassandraJournalConfig = CassandraJournal.this.config
        //}.initializePersistentConfig()
        log.debug("initialized HBaseSession successfully")
        sessionUsed = true
        s
      } catch {
        case NonFatal(e) =>
          // will be retried
          if (log.isDebugEnabled)
            log.debug("issue with initialization of CassandraSession, will be retried: {}", e.getMessage)
          closeSession(underlying)
          throw e
      }
    }
  }

  def session: Session = hbaseSession.underlying

  private def closeSession(session: Session): Unit = try {
    session.htable.close()
    //CassandraMetricsRegistry(context.system).removeMetrics(metricsCategory)
  } catch {
    case NonFatal(_) => // nothing we can do
  } finally {
    session.client.shutdown()
  }

  override def preStart(): Unit = {
    // eager initialization, but not from constructor
    self ! HBaseAsyncWriteJournal.Init
  }

  override def receivePluginInternal: Receive = {
    case WriteFinished(persistenceId, f) =>
      writeInProgress.remove(persistenceId, f)
    case HBaseAsyncWriteJournal.Init =>
      try {
        //hbaseSession
      } catch {
        case NonFatal(e) =>
          log.warning(
            "Failed to connect to Cassandra and initialize. It will be retried on demand. Caused by: {}",
            e.getMessage)
      }
  }

  override def postStop(): Unit = {
    if (sessionUsed)
      closeSession(session)
    super.postStop()
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
    writeInProgress.put(pid, p.future)

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
        session.flushWrites()
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
      session.executePut(
        RowKey(persistenceId, sequenceNr).toBytes,
        Array(PersistenceId, SequenceNr, Marker, Message),
        Array(toBytes(persistenceId), toBytes(sequenceNr), toBytes(AcceptedMarker), m.serialized.array))
    }
    Future.sequence(writes).map(_ => ())
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    writeInProgress.get(persistenceId) match {
      case null => super.asyncReadHighestSequenceNr(persistenceId, fromSequenceNr)
      case f    => f.flatMap(_ => super.asyncReadHighestSequenceNr(persistenceId, fromSequenceNr))
    }
  }

  // todo should be optimised to do ranged deletes
  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val watch = Stopwatch.createStarted()
    log.debug(s"AsyncDeleteMessagesTo for persistenceId: {} to sequenceNr: {} (inclusive)", persistenceId, toSequenceNr)

    def scanAndDeletePartition(part: Int, operator: ActorRef): Unit = {
      val toSeqNr = if (toSequenceNr < Long.MaxValue) toSequenceNr + 1 else Long.MaxValue
      val startScanRow = RowKey.firstInPartition(persistenceId) // 021-ID-000000000000000000
      val stopScanRow = RowKey.lastInPartition(persistenceId, toSeqNr) // 021-ID-9223372036854775800
      val persistenceIdRowRegex = RowKey.patternForProcessor(persistenceId) //  .*-ID-.*

      // we can avoid canning some partitions - guaranteed to be empty for smaller than the partition number seqNrs
      if (part > toSequenceNr)
        return

      log.debug("Scanning for keys to delete, start: {}, stop: {}, regex: {}", startScanRow.toKeyString, stopScanRow.toKeyString, persistenceIdRowRegex)

      val scan = new Scan
      scan.setStartRow(startScanRow.toBytes)
      scan.setStopRow(stopScanRow.toBytes)
      scan.setBatch(config.scanBatchSize)

      val fl = new FilterList()
      fl.addFilter(new FirstKeyOnlyFilter)
      fl.addFilter(new KeyOnlyFilter)
      fl.addFilter(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(persistenceIdRowRegex)))
      scan.setFilter(fl)

      val scanner = session.htable.getScanner(scan)
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

    val p = Promise[Unit]()
    val operator = context.actorOf(Operator.props(p, session.deleteRow, config.pluginDispatcherId))

    val partitions = config.partitionCount
    val partitionScans = (1 to partitions).map(partitionNr => Future { scanAndDeletePartition(partitionNr, operator) })
    Future.sequence(partitionScans) onComplete { _ => operator ! AllOpsSubmitted }

    p.future map { _ =>
      log.debug("Finished deleting messages for persistenceId: {}, to sequenceNr: {}, (took: {})", persistenceId, toSequenceNr, watch.stop())
      if (publishTestingEvents) context.system.eventStream.publish(FinishedDeletes(toSequenceNr))
    }
  }

  // end of journal plugin api impl ------------------------------------------------------------------------------------

  private def confirmAsync(persistenceId: String, sequenceNr: Long, channelId: String): Future[Unit] = {
    log.debug(s"Confirming async for persistenceId: {}, sequenceNr: {} and channelId: {}", persistenceId, sequenceNr, channelId)

    session.executePut(
      //RowKey(sequenceNr, persistenceId, sequenceNr).toBytes, // Old buggy code TODO to see if it cause old issues
      RowKey(persistenceId, sequenceNr).toBytes,
      Array(Marker),
      Array(confirmedMarkerBytes(channelId)))
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