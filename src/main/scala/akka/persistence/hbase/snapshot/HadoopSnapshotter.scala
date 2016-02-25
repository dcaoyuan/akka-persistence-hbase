package akka.persistence.hbase.snapshot

import akka.actor.{ ActorSystem, Extension }
import akka.persistence.{ SelectedSnapshot, SnapshotSelectionCriteria, SnapshotMetadata }
import akka.persistence.serialization.Snapshot
import akka.serialization.SerializationExtension
import scala.concurrent.Future
import scala.util.Try

/**
 * Common API for Snapshotter implementations. Used to provide an interface for the Extension.
 */
trait HadoopSnapshotter extends Extension {

  def system: ActorSystem

  val serialization = SerializationExtension(system)

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]]

  def saveAsync(meta: SnapshotMetadata, snapshot: Any): Future[Unit]

  def saved(metadata: SnapshotMetadata): Unit

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit]

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit]

  protected def deserialize(bytes: Array[Byte]): Try[Snapshot] =
    serialization.deserialize(bytes, classOf[Snapshot])

  protected def serialize(snapshot: Snapshot): Try[Array[Byte]] =
    serialization.serialize(snapshot)

}
