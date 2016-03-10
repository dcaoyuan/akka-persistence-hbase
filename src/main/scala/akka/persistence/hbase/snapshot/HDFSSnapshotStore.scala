package akka.persistence.hbase.snapshot

import akka.Done
import akka.actor.ActorLogging
import akka.actor.NoSerializationVerificationNeeded
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{ SelectedSnapshot, SnapshotSelectionCriteria, SnapshotMetadata }
import akka.persistence.serialization.Snapshot
import akka.serialization.SerializationExtension
import java.net.URI
import java.io.Closeable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileStatus, Path, FileSystem }
import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.IOUtils
import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.{ Try, Failure, Success }

object HDFSSnapshotStore {
  private case class WriteFinished(pid: String, f: Future[Done]) extends NoSerializationVerificationNeeded

  object HDFSSnapshotDescriptor {
    val SnapshotNamePattern = """snapshot-([a-zA-Z0-9]+)-([0-9]+)-([0-9]+)""".r

    def apply(meta: SnapshotMetadata): HDFSSnapshotDescriptor =
      HDFSSnapshotDescriptor(meta.persistenceId, meta.sequenceNr, meta.timestamp)

    def from(status: FileStatus): Option[HDFSSnapshotDescriptor] =
      FilenameUtils.getBaseName(status.getPath.toString) match {
        case SnapshotNamePattern(persistenceId, seqNumber, timestamp) =>
          Some(HDFSSnapshotDescriptor(persistenceId, seqNumber.toLong, timestamp.toLong))
        case _ =>
          None
      }
  }

  final case class HDFSSnapshotDescriptor(persistenceId: String, seqNumber: Long, timestamp: Long) {
    def toFilename = s"snapshot-$persistenceId-$seqNumber-$timestamp"
  }

}

class HDFSSnapshotStore(settings: HBaseSnapshotConfig) extends SnapshotStore with ActorLogging {
  import HDFSSnapshotStore._

  val serialization = SerializationExtension(context.system)

  implicit val executionContext = context.system.dispatchers.lookup("akka-hbase-persistence-dispatcher")

  private val conf = new Configuration
  private val fs = FileSystem.get(URI.create(settings.zookeeperQuorum), conf) // todo allow passing in all conf?

  /** Snapshots we're in progress of saving */
  private val writeInProgress = new java.util.HashMap[String, Future[Done]]()

  override def receivePluginInternal: Receive = {
    case WriteFinished(persistenceId, f) =>
      writeInProgress.remove(persistenceId, f)
    //    case HBaseAsyncWriteJournal.Init =>
    //      try {
    //        //hbaseSession
    //      } catch {
    //        case NonFatal(e) =>
    //          log.warning(
    //            "Failed to connect to Cassandra and initialize. It will be retried on demand. Caused by: {}",
    //            e.getMessage)
    //      }
  }

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    log.debug("[HDFS] Loading async, for persistenceId {}, criteria: {}", persistenceId, criteria)
    val snapshotMetas = listSnapshots(settings.snapshotHdfsDir, persistenceId)

    @tailrec
    def deserializeOrTryOlder(metas: List[HDFSSnapshotDescriptor]): Option[SelectedSnapshot] = metas match {
      case Nil =>
        None

      case desc :: tail =>
        tryLoadingSnapshot(desc) match {
          case Success(snapshot) =>
            Some(SelectedSnapshot(SnapshotMetadata(persistenceId, desc.seqNumber), snapshot))

          case Failure(ex) =>
            log.error(s"Failed to deserialize snapshot for $desc" + (if (tail.nonEmpty) ", trying previous one" else ""), ex)
            deserializeOrTryOlder(tail)
        }
    }

    // todo make configurable how many times we retry if deserialization fails (that's the take here)
    Future { deserializeOrTryOlder(snapshotMetas.take(3)) }
  }

  def saveAsync(meta: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val p = Promise[Done]

    val pid = meta.persistenceId
    writeInProgress.put(pid, p.future)

    if (writeInProgress.containsKey(pid)) {
      Future.failed(new Exception(s"Already working on persisting of $meta, aborting this (duplicate) request."))
    } else {
      val f = Future(serializeAndSave(meta, snapshot))
      writeInProgress.put(pid, p.future)

      f.onComplete { _ =>
        log.debug("Saved: {}", meta)
        self ! WriteFinished(pid, p.future)
      }(akka.dispatch.ExecutionContexts.sameThreadExecutionContext)

      f
    }
  }

  def deleteAsync(meta: SnapshotMetadata): Future[Unit] = {
    val pid = meta.persistenceId
    Future {
      val desc = HDFSSnapshotDescriptor(meta)
      fs.delete(new Path(settings.snapshotHdfsDir, desc.toFilename), true)
      log.debug("Deleted snapshot: {}", desc)
      writeInProgress.remove(pid)
    }
  }

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    Future {
      val toDelete = listSnapshots(settings.snapshotHdfsDir, persistenceId).dropWhile(_.seqNumber > criteria.maxSequenceNr)

      toDelete foreach { desc =>
        val path = new Path(settings.snapshotHdfsDir, desc.toFilename)
        fs.delete(path, true)
      }
    }
  }

  // internals --------

  /**
   * Looks for snapshots stored in directory for given `persistenceId`.
   * Guarantees that the returned list is sorted descending by the snapshots `seqNumber` (latest snapshot first).
   */
  private def listSnapshots(snapshotDir: String, persistenceId: String): List[HDFSSnapshotDescriptor] = {
    val descs = fs.listStatus(new Path(snapshotDir)) flatMap { HDFSSnapshotDescriptor.from }
    descs.sortBy(_.seqNumber).toList
  }

  private[snapshot] def serializeAndSave(meta: SnapshotMetadata, snapshot: Any) {
    val desc = HDFSSnapshotDescriptor(meta)

    serialization.serialize(Snapshot(snapshot)) match {
      case Success(bytes) => withStream(fs.create(newHDFSPath(desc))) { _.write(bytes) }
      case Failure(ex)    => log.error("Unable to serialize snapshot for meta: " + meta)
    }

  }

  private[snapshot] def tryLoadingSnapshot(desc: HDFSSnapshotDescriptor): Try[Snapshot] = {
    val path = new Path(settings.snapshotHdfsDir, desc.toFilename)
    deserialize(withStream(fs.open(path)) { IOUtils.toByteArray })
  }

  private def withStream[S <: Closeable, A](stream: S)(fun: S => A): A =
    try fun(stream) finally stream.close()

  private def newHDFSPath(desc: HDFSSnapshotDescriptor) =
    new Path(settings.snapshotHdfsDir, desc.toFilename)

  // TODO
  protected def deserialize(bytes: Array[Byte]): Try[Snapshot] =
    serialization.deserialize(bytes, classOf[Snapshot])

  protected def serialize(snapshot: Snapshot): Try[Array[Byte]] =
    serialization.serialize(snapshot)
}
